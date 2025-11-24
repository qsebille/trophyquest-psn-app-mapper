# TrophyQuest PSN App Mapper

Ce projet transforme les données PlayStation Network récupérées par le pipeline TrophyQuest en un schéma applicatif PostgreSQL. Le mapper utilise Apache Spark pour traiter les sorties brutes du fetcher et alimenter les tables liées aux jeux, trophées et utilisateurs.

## Prérequis
- Java 11+
- [sbt](https://www.scala-sbt.org/) 1.8+
- Accès à une instance PostgreSQL accessible depuis l'exécuteur du job

## Configuration
Les paramètres de connexion pour la base cible se trouvent dans `src/main/resources/config.properties` :

```
postgres.url=jdbc:postgresql://localhost:5432/trophyquest
postgres.user=postgres
postgres.password=postgres
```

Mettez ces valeurs à jour pour pointer vers votre base. La journalisation est configurée via `src/main/resources/log4j2.properties`.

## Exécuter un job
Le point d'entrée est `com.trophyquest.TrophyQuestMapperMain`. Utilisez l'argument `--jobName` pour sélectionner le mapper à lancer :

- `game`
- `trophy_collection`
- `trophy`
- `user_game`
- `user_profile`
- `user_trophy_collection`
- `user_trophy`
- `all` (lance tous les jobs en séquence)

Exemple (exécution locale Spark en utilisant tous les cœurs) :

```
sbt "run --jobName all"
```

Chaque job journalise sa progression et s'arrête lorsque la session Spark se termine.

## Développement
Exécutez la suite de tests avec :

```
sbt test
```

Pour ajuster les paramètres Spark (par exemple la mémoire des executors), modifiez la configuration du builder dans `TrophyQuestMapperMain` avant d'exécuter.
