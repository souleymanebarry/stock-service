# 📦 stock-service — Migration JSON → Avro

> Documentation de la migration de la sérialisation Kafka de **JSON** vers **Apache Avro + Confluent Schema Registry**, du fonctionnement du service, et des points d'attention.

---

## 1. Pourquoi cette migration ?

Avant, `stock-service` sérialisait ses événements Kafka en **JSON** (`JsonSerializer` / `JsonDeserializer` de Spring Kafka), avec des POJO Lombok maison dans `share/event/`.

Problèmes de cette approche dans une architecture Saga multi-services :

- **Aucun contrat fort** : chaque service redéfinit ses propres classes d'événements. Un champ renommé d'un côté casse silencieusement l'autre.
- **Pas de validation de schéma** : un message mal formé n'est détecté qu'au runtime, côté consommateur.
- **Payload verbeux** : le JSON transporte les noms de champs à chaque message.
- **Pas de gestion de compatibilité** : impossible de faire évoluer un événement sans risque de casse.

Avro + Schema Registry résout tout cela :

- **Contrat partagé et versionné** : un seul schéma `.avsc` fait foi pour tous les services.
- **Validation centralisée** : le Schema Registry rejette les schémas incompatibles.
- **Payload binaire compact** : seul l'ID de schéma est transporté, pas les noms de champs.
- **Compatibilité contrôlée** (BACKWARD / FORWARD) : les événements peuvent évoluer sans casser les consommateurs existants.

L'écosystème Avro **existait déjà** dans le projet `saga-pattern/` (module `retail-event-schema`, `orders-service`, `catalog-service`). Cette migration **aligne `stock-service` sur ce contrat existant**.

---

## 2. Le contrat partagé : `retail-event-schema`

Les schémas Avro ne sont **pas** définis dans `stock-service`. Ils vivent dans un module Maven dédié et partagé :

```
saga-pattern/retail-event-schema/
└── src/main/avro/
    ├── order/OrderPlacedEvent.avsc      ← consommé par stock-service
    ├── stock/StockReservedEvent.avsc    ← produit par stock-service
    └── stock/StockRejectedEvent.avsc    ← produit par stock-service
```

Ce module utilise l'`avro-maven-plugin` pour **générer les classes Java** (`SpecificRecord`) à partir des `.avsc`, puis est publié comme **jar** :

```
com.barry.saga.retail:retail-event-schema:1.0-SNAPSHOT
```

`stock-service` consomme simplement ce jar. **Zéro duplication de schéma.**

### Mapping des types Avro → Java

| Champ Avro | Type Avro | Type Java généré |
|---|---|---|
| `orderId`, `sku`, `eventId`, `customerId`, `idempotencyKey` | `string` | `java.lang.String` |
| `quantity` | `int` | `int` / `Integer` |
| `unitPrice`, `totalAmount` | `bytes` + `logicalType: decimal` | `java.math.BigDecimal` |
| `createdAt`, `reservedAt`, `rejectedAt` | `long` + `logicalType: timestamp-millis` | `java.time.Instant` |

> ⚠️ Deux conséquences importantes dans le code métier :
> - `orderId` est un **`String`** côté Avro → on fait `UUID.fromString(...)` avant de toucher la base (l'entité JPA utilise un `UUID`).
> - Les dates sont des **`Instant`** (et non `LocalDateTime`) → on utilise `Instant.now()`.

---

## 3. Ce qui a changé dans `stock-service`

### 3.1 `pom.xml`

```xml
<!-- Repository Confluent (obligatoire pour kafka-avro-serializer) -->
<repositories>
  <repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
  </repository>
</repositories>

<dependencies>
  <!-- Sérialisation Avro + Schema Registry -->
  <dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.5.4</version>
  </dependency>
  <dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.11.3</version>
  </dependency>
  <!-- Contrat d'événements partagé (classes Avro générées) -->
  <dependency>
    <groupId>com.barry.saga.retail</groupId>
    <artifactId>retail-event-schema</artifactId>
    <version>1.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

### 3.2 `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    properties:
      schema.registry.url: http://localhost:8081   # ← Confluent Schema Registry
```

### 3.3 Producer — `KafkaProducerConfig`

| Avant (JSON) | Après (Avro) |
|---|---|
| `VALUE_SERIALIZER = JsonSerializer` | `VALUE_SERIALIZER = KafkaAvroSerializer` |
| `KafkaTemplate<String, Object>` | `KafkaTemplate<String, SpecificRecord>` |
| — | `+ schema.registry.url` |
| — | `+ enable.idempotence = true` |

### 3.4 Consumer — `KafkaConsumerConfig`

| Avant (JSON) | Après (Avro) |
|---|---|
| `JsonDeserializer<>(OrderPlacedEvent.class)` | `KafkaAvroDeserializer` enveloppé dans `ErrorHandlingDeserializer` |
| `addTrustedPackages(...)` | `specific.avro.reader = true` (résolution via Schema Registry) |
| auto-commit implicite | **ack manuel** (`AckMode.MANUAL`) + `DefaultErrorHandler` |

### 3.5 Code métier

- `StockEventProducer` : envoie des `SpecificRecord`, **clé Kafka = `orderId`** (garantit l'ordre des événements d'une même commande sur une même partition — essentiel au Saga), avec headers `eventType` / `schemaVersion`.
- `StockOrderListener` : consomme l'`OrderPlacedEvent` Avro et **acquitte manuellement** (`ack.acknowledge()`).
- `StockServiceImpl` : construit les événements via `XxxEvent.newBuilder()...build()` (builder Avro), renseigne `eventId` (UUID), `version`, `idempotencyKey`, et utilise `Instant.now()`.
- **Supprimés** : les 3 anciens POJO Lombok dans `share/event/` (`OrderPlacedEvent`, `StockReservedEvent`, `StockRejectedEvent`).

---

## 4. Fonctionnement du `stock-service`

### Rôle dans le Saga

`stock-service` est un **participant** du Saga de création de commande (pas l'orchestrateur). Il réserve le stock et notifie le résultat.

### Flux nominal

```
                   ┌──────────────────────────────────────────────┐
                   │                 stock-service                │
                   │                                              │
 order.placed ────►│ StockOrderListener.onOrderPlaced()           │
 (OrderPlaced)     │        │                                     │
                   │        ▼                                     │
                   │ StockServiceImpl.reservedStock()  @Transactional
                   │        │                                     │
                   │        ├─ pour chaque item :                 │
                   │        │   • findStockItem(sku)              │
                   │        │   • si stock insuffisant ──┐        │
                   │        │   • sinon : updateStock +  │        │
                   │        │     saveReservation(RESERVED)        │
                   │        │                            │        │
                   │        ▼                            ▼        │
                   │ publishStockReserved()      publishStockRejected()
                   │        │                            │        │
                   └────────┼────────────────────────────┼────────┘
                            ▼                            ▼
                     stock.reserved              stock.rejected
                   (StockReservedEvent)        (StockRejectedEvent)
                            │                            │
                            └──────────► orders-service ◄┘
                                       (met à jour la commande)
```

### Étapes détaillées

1. **Réception** — `StockOrderListener` consomme un `OrderPlacedEvent` sur le topic `order.placed`.
2. **Traitement transactionnel** — `reservedStock()` itère sur les items de la commande :
   - récupère le `StockItemEntity` par SKU ;
   - si la quantité disponible est insuffisante → enregistre une réservation `REJECTED`, publie un `StockRejectedEvent`, et **arrête le Saga** (`return`) ;
   - sinon → décrémente `availableQuantity`, incrémente `reservedQuantity`, enregistre une réservation `RESERVED`.
3. **Publication du résultat** :
   - tous les items réservés → `StockReservedEvent` sur `stock.reserved` ;
   - un seul item en échec → `StockRejectedEvent` sur `stock.rejected`.
4. **Acquittement** — le listener fait `ack.acknowledge()` (commit manuel de l'offset Kafka).
5. **Historique** — chaque tentative est tracée dans la table `stock_reservations` (`StockReservationEntity`), ce qui rend le Saga auditable et facilite les compensations.

### Atomicité

`reservedStock()` est annoté `@Transactional` : **soit toutes les réservations d'une commande réussissent, soit aucune n'est validée** en base. La publication Kafka, elle, intervient après la logique métier (voir limite « dual write » plus bas).

### Topics

| Topic | Sens | Événement |
|---|---|---|
| `order.placed` | consommé | `OrderPlacedEvent` |
| `stock.reserved` | produit | `StockReservedEvent` |
| `stock.rejected` | produit | `StockRejectedEvent` |

---

## 5. ⚠️ Points d'attention

### 5.1 Le format de transport a changé (JSON → binaire Avro)

Les anciens messages **JSON** présents dans les topics ne sont **plus désérialisables** par le nouveau consumer Avro.

- Comme le consumer démarre avec `auto.offset.reset = earliest`, il tenterait de rejouer d'anciens messages JSON → erreurs de désérialisation.
- Le `DefaultErrorHandler` les **logge sans bloquer** la consommation (pas de poison pill), mais pour repartir propre :
  - **vider / recréer les topics** (`order.placed`, `stock.reserved`, `stock.rejected`), ou
  - utiliser un **nouveau `group.id`** + `latest`, ou
  - purger les messages legacy.

### 5.2 Infrastructure requise au runtime

Avro avec Schema Registry impose **deux dépendances d'infra** :

- **Kafka** sur `localhost:9092`
- **Confluent Schema Registry** sur `localhost:8081`

Les deux sont fournis par le `docker-compose.yml` du projet `saga-pattern/` :

```bash
cd saga-pattern
docker compose up -d zookeeper kafka schema-registry
```

> Si le Schema Registry est injoignable au démarrage, le producer/consumer échouera à (dé)sérialiser. Vérifier : `curl http://localhost:8081/subjects`.

### 5.3 Couplage au jar `retail-event-schema:1.0-SNAPSHOT`

`stock-service` dépend d'un **SNAPSHOT local** non publié sur un dépôt distant. Il doit donc être présent dans le `~/.m2` local.

- Sur la machine actuelle : ✅ déjà présent (le projet `saga-pattern` a été buildé).
- Sur un **clone neuf** : il faut d'abord installer le module de schémas :

```bash
cd saga-pattern/retail-event-schema
mvn clean install
```

> À terme, pour un vrai déploiement, publier `retail-event-schema` sur un dépôt Maven partagé (Nexus / Artifactory / GitHub Packages) plutôt qu'en SNAPSHOT local.

### 5.4 Cohérence du contrat entre services

Comme tous les services partagent le **même** `retail-event-schema`, faire évoluer un `.avsc` impacte **tout le monde**. Règles :

- Toute évolution doit respecter la **compatibilité** configurée dans le Schema Registry (par défaut `BACKWARD`).
- Ajouter un champ ⇒ lui donner une **valeur par défaut** (comme `version: "v1"`).
- Ne **jamais** supprimer/renommer un champ sans stratégie de compatibilité.
- Rebuild + republier le jar, puis rebuild les services consommateurs.

### 5.5 Ordre des événements (clé Kafka)

Le producer utilise **`orderId` comme clé Kafka**. C'est volontaire : tous les événements d'une même commande tombent sur la **même partition**, donc sont **ordonnés**. Ne pas changer la clé sans mesurer l'impact sur les garanties d'ordre du Saga.

### 5.6 Limite connue : « dual write » (Kafka + DB)

Le service écrit en **base** (réservations) **puis** publie sur **Kafka**, dans deux systèmes distincts. Si le process tombe entre les deux, la base et Kafka peuvent diverger. Cette migration ne résout **pas** ce point.

> Amélioration future possible : **Transactional Outbox** (écrire l'événement dans une table outbox dans la même transaction, puis relayer vers Kafka via un CDC type Debezium).

---

## 6. Vérifier la migration

```bash
# 1. (une seule fois) installer le contrat partagé si absent du .m2
cd saga-pattern/retail-event-schema && mvn clean install

# 2. compiler stock-service
cd stock_service && ./mvnw clean compile        # → BUILD SUCCESS

# 3. démarrer l'infra
cd saga-pattern && docker compose up -d zookeeper kafka schema-registry

# 4. lancer le service
cd stock_service && ./mvnw spring-boot:run

# 5. inspecter les schémas enregistrés
curl http://localhost:8081/subjects
# attendu (après le 1er message) :
#   ["order.placed-value","stock.reserved-value","stock.rejected-value"]
```

---

## 7. Récapitulatif des fichiers touchés

| Fichier | Action |
|---|---|
| `pom.xml` | repo Confluent + dépendances Avro / Schema Registry / contrat partagé |
| `src/main/resources/application.yml` | + `schema.registry.url` |
| `kafka/config/KafkaProducerConfig.java` | `KafkaAvroSerializer`, `SpecificRecord`, idempotence |
| `kafka/config/KafkaConsumerConfig.java` | `KafkaAvroDeserializer`, ack manuel, error handler |
| `kafka/StockEventProducer.java` | envoi `SpecificRecord`, clé = `orderId`, headers |
| `kafka/listener/StockOrderListener.java` | consomme l'`OrderPlacedEvent` Avro + ack manuel |
| `service/StockService.java` | import du contrat Avro |
| `service/impl/StockServiceImpl.java` | builders Avro, `Instant`, `UUID.fromString` |
| `share/event/*.java` | **supprimés** (anciens POJO JSON) |
