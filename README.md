**📦 stock-service**

**🎯 Rôle du service**

stock-service est responsable de la gestion des stocks produits dans le système.

Il intervient dans le Saga de création de commande pour :

* vérifier la disponibilité des produits
* réserver les quantités demandées
* notifier le résultat via Kafka

👉 C’est un participant clé du Saga, mais pas l’orchestrateur.
----------------------------------------------------------------
**🧩 Responsabilités**

✅ **Ce que fait** stock-service
* Consomme les événements OrderPlacedEvent
* Vérifie la disponibilité des stocks par SKU
* Réserve le stock si possible
* Publie :
  StockReservedEvent si succès
  StockRejectedEvent si échec
* Conserve l’historique des réservations (Saga-friendly)
----------------------------------------------------------------
**📚 Documentation**

* [Migration JSON → Avro, fonctionnement & points d'attention](docs/MIGRATION_AVRO.md)
