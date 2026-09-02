import {
  createBuilder,
  createSchema,
  number,
  relationships,
  string,
  table,
} from '@rocicorp/zero';

export const topic = table('topics')
  .columns({
    id: number(),
    name: string(),
  })
  .primaryKey('id');

export const user = table('users')
  .columns({
    id: number(),
    name: string(),
  })
  .primaryKey('id');

export const message = table('messages')
  .columns({
    id: number(),
    topicId: number().from('topic_id'),
    userId: number().from('user_id'),
    content: string(),
    // timestamptz wird von Zero auf number() abgebildet (Epoch-Millisekunden).
    createdAt: number().from('created_at'),
    matchValue: number().from('match_value'),
  })
  .primaryKey('id');

const messageRelationships = relationships(message, ({one}) => ({
  topic: one({
    sourceField: ['topicId'],
    destField: ['id'],
    destSchema: topic,
  }),
  author: one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

export const schema = createSchema({
  tables: [topic, user, message],
  relationships: [messageRelationships],
});

export type Schema = typeof schema;

export const zql = createBuilder(schema);

declare module '@rocicorp/zero' {
  interface DefaultTypes {
    schema: Schema;
  }
}
