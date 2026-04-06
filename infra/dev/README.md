# Dev Deployment Notes

These settings are required on the production-like dev server and should not be
reverted during deploys.

## Docker Compose

- `/Users/esma/Dev/DZ_fastapi/infra/dev/docker-compose.dev.yaml`
  must keep the frontend binding as:

```yaml
ports:
  - "127.0.0.1:3000:80"
```

This keeps the frontend reachable only through the host `nginx` on ports `80`
and `443`.

## Server `.env`

The server-side `/root/DZ_fastapi/.env` must contain:

```env
AUTH_COOKIE_SECURE=true
API_DOCS_ENABLED=false
```

## Expected Public Ports

After deploy, only these ports should be publicly reachable:

- `22`
- `80`
- `443`

These ports must stay closed from the internet:

- `3000`
- `8000`
- `5432`
- `6379`
