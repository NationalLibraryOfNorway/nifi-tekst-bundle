# NiFi tekst bundle

Bundle av prosessorer laget for produksjonsløyper for tekstmateriale.

## Lokal utvikling med Docker

### Forutsetninger

* Docker og Docker Compose
* JDK 21 og Maven (for å bygge NAR-pakken)
* [Git LFS](https://git-lfs.com/) (for å laste ned testfiler med store størrelser).
  * [Installeringsinstruksjoner](https://github.com/git-lfs/git-lfs?tab=readme-ov-file#installing)
  * OBS: Installering av Git LFS må gjøres før kloning av repoet, ellers vil ikke testfiler lastes ned. Hvis du allerede har klonet repoet, kjør `git lfs install` og deretter `git lfs pull` i repo-mappen.

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

Bruk flowen **nifi-tekst-bundle-dev** for å utvikle og teste prosessorene lokalt.

Denne flowen skal vi bruke for lokal utvikling, og du kan bruke volume mountene (se under) for å teste med filer og output lokalt.

#### NiFi-flowen hentes fra et GitHub Registry. 
1. Hamburger meny -> Controller Settings -> Registry Clients (fane) -> Add new registry client (+ knapp)
2. Velg "GitHubFlowRegistryClient" som type, og "add".
3. Trykk på kebab-menyen til høyre for den nye registry clienten, og velg "Edit".
4. Velg fane "Properties", og legg inn registry verdier.
   1. "Authentication Type": "App Installation"
      1. Autentiseringshemmeligheter for Github "appen" ligger i Vault `github_internal_bot`.
   2. "Github API URL", "Repository Owner" og "Repository Name" er verdier til den **interne** github instansen vår med ett github repo som registry. (se properties i stage/prod for eksempel)
5. Gå tilbake til hoved "canvas".
6. Dra ut **skyen** med **pil ned** ikonet i øvre venstre hjørne, og slipp den på canvaset.
7. Velg **"nifi-tekst-bundle-dev"** i **flow** dropdownen, og trykk "Import".

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

Utrulling skjer via opplastning til Artifactory, og deretter nedlastning fra NiFi i stage/prod miljøene.

Bruk NiFi flowen "Update NAR Packages"

## Vedlikehold

Tekst-teamet på Nasjonalbibliotekets IT-avdeling vedlikeholder NiFi-prosessorene.
Alle kan lage issues, men vi kan ikke garantere at alle blir tatt tak i.
Interne behov går foran eksterne forespørsler.
