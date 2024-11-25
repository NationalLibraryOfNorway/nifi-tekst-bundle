# NiFi tekst bundle

Bundle av prosessorer laget for produksjonsløyper for tekstmateriale.

## Lokal utvikling

* Installer nifi, JDK 11 og Maven lokalt
* Lag prosessorer under `nifi-text-bundle-processors`-modulen
* Kjør `mvn clean install` for å bygge prosessorene
* Kopier nar til nifi sin `lib`-mappe
  (eks. `cp nifi-tekst-bundle-nar/target/nifi-tekst-bundle-nar-1.0.0.nar /opt/nifi/lib/`)
* Start/restart nifi
* Åpne nifi GUIet lokalt (default `https://127.0.0.1:8443/nifi/`) og se din prosessor i listen

## Utrulling

Pr. nå er det ingen faste metoder for å rulle ut nye NARs.
NAR-pakkene kan enkelt kopieres til din NiFi-server etter å ha blitt bygget lokalt.

## Vedlikehold
Tekst-teamet på Nasjonalbibliotekets IT-avdeling vedlikeholder NiFi-prosessorene.
Alle kan lage issues, men vi kan ikke garantere at alle blir tatt tak i.
Interne behov går foran eksterne forespørsler.

NB-ansatte kan melde feil/mangler/forslag via servicedesken.