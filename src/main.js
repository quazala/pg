import { Pool } from 'pg';

export const connect = async (ctx) => {
  const { logger } = ctx;
  logger.info('Attempting to connect to PostgreSQL');
  const pool = new Pool({
    connectionString: ctx.env.postgres.url,
  });

  try {
    const client = await pool.connect();
    try {
      await client.query('SELECT NOW()');
      logger.info('Successfully connected to PostgreSQL');
    } finally {
      client.release();
    }

    ctx.dbs.postgres = {
      pool,
    };
  } catch (error) {
    logger.error(`Failed to connect to PostgreSQL: ${error.message}`, 'error');
    throw error;
  }
};

export const disconnect = async (ctx) => {
  const { logger } = ctx;
  if (ctx.dbs.postgres?.pool) {
    logger.info('Disconnecting from PostgreSQL');
    try {
      await ctx.dbs.postgres.pool.end();
      logger.info('Successfully disconnected from PostgreSQL');
    } catch (error) {
      logger.info(`Error during PostgreSQL disconnection: ${error.message}`, 'error');
      throw error;
    }
  } else {
    logger.warn('No active PostgreSQL connection to disconnect', 'warn');
  }
};
