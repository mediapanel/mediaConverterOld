## Requirements

- docker-compose
- vieux/docker-volume-sshfs
  - installable by `docker plugin install vieux/sshfs`

## Configuration

All required environment variables are in `.env.default`, which should be
edited and copied over to `.env`.

## Running

```sh
docker-compose up -d
```
