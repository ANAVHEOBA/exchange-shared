# Assetar Postman Docs

OpenAPI is the source of truth. The Postman collection is generated from:

- Local: `http://localhost:3000/api-docs/openapi.json`
- Production: `https://exchange-shared-production.up.railway.app/api-docs/openapi.json`

## Generate Collection

Install the locked converter dependency once:

```sh
npm ci
```

Start the backend, then run:

```sh
npm run postman:collection
```

For production:

```sh
npm run postman:collection:prod
```

Or override the source explicitly:

```sh
OPENAPI_URL=https://your-api-host/api-docs/openapi.json npm run postman:collection
```

If you run the backend on a non-default local port:

```sh
PORT=8080 npm run postman:collection
```

This writes:

- `postman/assetar.openapi.json`
- `postman/Assetar.postman_collection.json`

Those two generated files should be regenerated from the current backend before sharing or committing a final Postman export.

Validate generated JSON:

```sh
npm run postman:validate
```

## Import Into Postman

Import these files:

- `postman/Assetar.postman_collection.json`
- `postman/Assetar.local.postman_environment.json`
- `postman/Assetar.production.postman_environment.json`

Set the active environment to `Assetar Local` or `Assetar Production`.

The environments define both `baseUrl` and `base_url`. The OpenAPI-to-Postman converter usually emits `baseUrl`; `base_url` is kept for team scripts and manual requests.

## Security

Keep operations endpoints private. Do not publish admin/ops documentation publicly unless the workspace and documentation are access-controlled.
