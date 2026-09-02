import {serve} from '@hono/node-server';
import {Hono} from 'hono';
import {handleQueryRequest} from '@rocicorp/zero/server';
import {mustGetQuery} from '@rocicorp/zero';
import {queries} from './queries.ts';
import {schema} from './schema.ts';

const app = new Hono();

app.get('/healthz', c => c.text('ok'));

app.post('/api/zero/query', async c => {
  const result = await handleQueryRequest({
    handler: (name, args) => {
      const query = mustGetQuery(queries, name);
      return query.fn({args});
    },
    schema,
    request: c.req.raw,
    userID: null,
  });

  return c.json(result);
});

const port = Number(process.env.PORT ?? 3000);
serve({fetch: app.fetch, port});
console.error(`zero query API listening on :${port}`);
