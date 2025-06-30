import PostgreSQLClient, { TABLES } from "./src/postgres-client";

const DB_ID = "bseocOYqflQMDdju75d";

async function main() {
  const pgClient = new PostgreSQLClient();
  try {
    const result = await pgClient.query(`
        SELECT * FROM "${DB_ID}".${TABLES.PROJECTS}
        ORDER BY __auto_number ASC
        LIMIT 100
    `);
    console.log(result.length);
  } catch (error) {
    console.error(error);
  } finally {
    await pgClient.close();
  }
}

main();
