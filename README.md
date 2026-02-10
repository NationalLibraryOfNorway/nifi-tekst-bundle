# NiFi tekst bundle

Bundle av prosessorer laget for produksjonsløyper for tekstmateriale.

## Lokal utvikling med Docker

### Forutsetninger

* Docker og Docker Compose
* JDK 21 og Maven (for å bygge NAR-pakken)

### Bygg og start

```bash
# Bygg NAR-pakken
mvn clean package -DskipTests

# Start NiFi med docker-compose
docker compose up -d
```

NiFi vil være tilgjengelig på https://localhost:8443/nifi/

**Innlogging:**

- Brukernavn: `admin`
- Passord: `adminadminadmin`

### GitHub Registry for flow-konfigurasjon

Bruk flowen 'nifi-tekst-bundle-dev' for å utvikle og teste prosessorene lokalt.
NiFi-flowen hentes fra et GitHub Registry. Autentiseringshemmeligheter for Github "appen" ligger i Vault (`github_internal_bot`).
1. Burger meny -> Controller Settings -> Registry Clients (fane) -> Add new registry client (+ knapp)
2. Velg "GitHubFlowRegistryClient" som type, og fyll inn informasjonen. Se stage/prod for eksempel på hvordan dette er satt opp.

### Volume mounts

| Mount                                               | Container-sti                           | Beskrivelse                                       |
|-----------------------------------------------------|-----------------------------------------|---------------------------------------------------|
| `./nifi-tekst-bundle-nar/target`                    | `/opt/nifi/nifi-current/nar_extensions` | NAR-pakken med custom prosessorer (read-only)     |
| `./nifi-tekst-bundle-processors/src/test/resources` | `/data/test-resources`                  | Testfiler for utvikling og testing (read-only)    |
| `./nifi-docker/output`                              | `/data/output`                          | Mappe for output fra NiFi-flows                   |

### Rebuild etter endringer

Etter endringer i prosessor-koden:

```bash
mvn clean package -DskipTests
docker compose restart nifi
```

## Utrulling

Pr. nå er det ingen faste metoder for å rulle ut nye NARs.
NAR-pakkene kan enkelt kopieres til din NiFi-server etter å ha blitt bygget lokalt.

## Vedlikehold

Tekst-teamet på Nasjonalbibliotekets IT-avdeling vedlikeholder NiFi-prosessorene.
Alle kan lage issues, men vi kan ikke garantere at alle blir tatt tak i.
Interne behov går foran eksterne forespørsler.
