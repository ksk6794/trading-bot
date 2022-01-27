## Requirements
https://docs.docker.com/engine/install/


## Installation
Configure development environment:
https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html


### Create `.env` and specify your API keys

```bash
cat <<EOF > .env
BINANCE_PUBLIC_KEY=
BINANCE_PRIVATE_KEY=
EOF
```

### Create external volumes
```bash
docker volume create mongodb
docker volume create portainer
```

### Run
```bash
docker compose -f production.yml up
```

### Connect to MongoDB
```bash
docker exec -it <container_id> mongodb mongodb://root:1111@localhost
```

## Dashboards

**Portainer:** http://localhost:9000

**RabbitMQ Management:** http://localhost:15672
