import { Graph, IdUtils, SystemIds, type Op } from '@graphprotocol/grc-20';
import * as fs from "fs";
//import { publish } from './src/publish';
import { addSpace, cleanText, fetchWithRetry, filterOps, GEO_IDS, getSpaces, normalizeToUUID, normalizeToUUID_STRING, propertyToIdMap, propertyToTypeIdMap, readAllOpsFromFolder, testnetWalletAddress, typeToIdMap } from './src/constants_v2';
import path from 'path';
import PostgreSQLClient, { TABLES, DB_ID } from "./src/postgres-client";
import { processEntity } from './post_entity';
import { printOps, publishOps, searchEntities } from './src/functions';


const offset = 0
const limit = 100
const pgClient = new PostgreSQLClient();
// global or passed down as a parameter

console.log("HERE")
process.on('SIGINT', async () => {
    console.log("FINAL OFFSET: ", offset);
    console.log('Exiting gracefully...');
    await pgClient.close();
    process.exit(0);
});

try {

    const tables = await pgClient.query(`
      SELECT 
        table_name,
        json_agg(
          json_build_object(
            'column', column_name,
            'type', data_type
          ) ORDER BY ordinal_position
        ) AS columns
      FROM information_schema.columns
      WHERE table_schema = 'crypto'
      GROUP BY table_name
      ORDER BY table_name;
    `);
    // Convert to formatted JSON string
    const tables_with_columns = JSON.stringify(tables, null, 2); // 2-space indentation
    fs.writeFileSync("tables_with_columns.txt", tables_with_columns, "utf-8");


} catch (error) {
    console.error(error);
} finally {
    await pgClient.close();
}



