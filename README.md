# smart-schedulers

`smart-schedulers` to osobny worker, który:
- działa non-stop,
- co minutę wylicza aktywne sloty schedulerów,
- dla slotów z progiem mocy sprawdza świeżość i wartość ostatniego pomiaru providera,
- wysyła komendę `DEVICE_COMMAND` (`is_on=true`) do agenta i czeka na ACK,
- zapisuje wynik do `device_events` jako `event_type=SCHEDULER`.

## Uruchomienie lokalne

```bash
cd smart-schedulers
docker compose up --build
```

Serwis jest samodzielny (`build: .`) i działa z katalogu `smart-schedulers`.

## Git submodule `smart_common` (repo referencyjne)

W katalogu serwisu przygotowana jest konfiguracja submodułu pod:
`https://github.com/KennyDaktyl/smart_common`.

Inicjalizacja:

```bash
cd smart-schedulers
git submodule sync --recursive
git submodule update --init --recursive
```

albo:

```bash
./scripts/init_smart_common_submodule.sh
```

Submoduł jest referencyjny (`vendor/smart_common`), a runtime schedulera używa
lokalnego pakietu `smart_common` w tym serwisie (modele/schematy/usługi schedulera).

## Parametry runtime

- `ENV`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_NAME`
- `REDIS_HOST`
- `REDIS_PORT`
- `NATS_URL`
- `STREAM_NAME`
- `BACKEND_PORT`
- `PORT`
- `LOG_DIR`
- `SCHEDULER_ACK_TIMEOUT_SEC` (domyślnie `10`)
- `SCHEDULER_MAX_CONCURRENCY` (domyślnie `25`)
- `SCHEDULER_IDEMPOTENCY_TTL_SEC` (domyślnie `120`)
- `SCHEDULER_REDIS_PREFIX` (domyślnie `smart-schedulers`)
