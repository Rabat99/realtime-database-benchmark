import {defineQueries, defineQuery} from '@rocicorp/zero';
import {z} from 'zod';
import {zql} from './schema.ts';

/*
 * Gemeinsame Query-Registry fuer Performance- und Expressivitaetsmessungen.
 * Die Join-Anfragen verwenden die Relation `author` aus schema.ts.
 *
 * Die Proben fuer OFFSET, COUNT und MAX pruefen den jeweiligen Operator bei
 * der Registrierung gegen das installierte ZQL-Queryobjekt. Ein fehlender
 * Operator wird mit `__NOT_EXPRESSIBLE__` gekennzeichnet. Andere Fehler bleiben
 * technische Testfehler.
 */

function requireQueryMethod(
  query: any,
  method: string,
  args: readonly unknown[],
  label: string,
): any {
  const fn = query?.[method];

  if (typeof fn !== 'function') {
    throw new Error(
      `__NOT_EXPRESSIBLE__:${label}: ` +
      `ZQL query object has no ${method}() method in this installed version`,
    );
  }

  return fn.apply(query, args);
}

export const queries = defineQueries({
  bench: {
    // ---------------------------------------------------------------------
    // Performance-Klassen
    // ---------------------------------------------------------------------

    byTopic: defineQuery(
      z.object({topicId: z.number()}),
      ({args: {topicId}}) =>
        zql.messages.where('topicId', topicId),
    ),

    byTopicSorted: defineQuery(
      z.object({topicId: z.number()}),
      ({args: {topicId}}) =>
        zql.messages
          .where('topicId', topicId)
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc'),
    ),

    byTopicWindow: defineQuery(
      z.object({
        topicId: z.number(),
        limit: z.number(),
      }),
      ({args: {topicId, limit}}) =>
        zql.messages
          .where('topicId', topicId)
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc')
          .limit(limit),
    ),

    byTopicWindowJoin: defineQuery(
      z.object({
        topicId: z.number(),
        limit: z.number(),
      }),
      ({args: {topicId, limit}}) =>
        zql.messages
          .where('topicId', topicId)
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc')
          .limit(limit)
          .related('author', q => q.one()),
    ),

    byTopicWindowSearch: defineQuery(
      z.object({
        topicId: z.number(),
        needle: z.string(),
        limit: z.number(),
      }),
      ({args: {topicId, needle, limit}}) =>
        zql.messages
          .where('topicId', topicId)
          .where('content', 'ILIKE', `%${needle}%`)
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc')
          .limit(limit),
    ),

    // ---------------------------------------------------------------------
    // Expressivitaet: zusammengesetzte Filter
    // ---------------------------------------------------------------------

    compositeAnd: defineQuery(
      z.object({
        topicId: z.number(),
        userId: z.number(),
      }),
      ({args: {topicId, userId}}) =>
        zql.messages
          .where('topicId', topicId)
          .where('userId', userId),
    ),

    compositeOr: defineQuery(
      z.object({
        topicId: z.number(),
        userA: z.number(),
        userB: z.number(),
      }),
      ({args: {topicId, userA, userB}}) =>
        zql.messages
          .where('topicId', topicId)
          .where(({cmp, or}) =>
            or(
              cmp('userId', userA),
              cmp('userId', userB),
            ),
          ),
    ),

    // ---------------------------------------------------------------------
    // Expressivitaet: Capability-Probes
    //
    // Die lokale Typaufhebung erlaubt die Laufzeitpruefung optionaler Operatoren.
    // ---------------------------------------------------------------------

    offsetProbe: defineQuery(
      z.object({
        topicId: z.number(),
        limit: z.number(),
        offset: z.number(),
      }),
      ({args: {topicId, limit, offset}}) => {
        const sorted = zql.messages
          .where('topicId', topicId)
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc');

        const shifted = requireQueryMethod(
          sorted,
          'offset',
          [offset],
          'OFFSET',
        );

        return shifted.limit(limit);
      },
    ),

    countProbe: defineQuery(
      z.object({
        topicId: z.number(),
      }),
      ({args: {topicId}}) => {
        const filtered = zql.messages.where('topicId', topicId);

        return requireQueryMethod(
          filtered,
          'count',
          [],
          'COUNT',
        );
      },
    ),

    maxProbe: defineQuery(
      z.object({
        topicId: z.number(),
      }),
      ({args: {topicId}}) => {
        const filtered = zql.messages.where('topicId', topicId);

        return requireQueryMethod(
          filtered,
          'max',
          ['id'],
          'MAX',
        );
      },
    ),
  },
});
