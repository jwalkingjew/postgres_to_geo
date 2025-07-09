// To do
// - Do we want to add the project avatar as the avatar in the funding round entity?

import { TeableClient } from "./src/teable-client";
import { PROD_TABLE_IDS } from "./src/teable-meta";
import { testnetWalletAddress, getSpaces, searchEntity, filterOps, normalizeToUUID, normalizeToUUID_STRING, propertyToIdMap, GEO_IDS, readAllOpsFromFolder, searchEntities, propertyToTypeIdMap, searchEntities_byId } from "./src/constants_v2";
import { processInvestment } from "./ethcc_post_investment";
import { processProject } from "./ethcc_post_project";
import * as fs from "fs";
import { publish } from "./src/publish";
import { SystemIds, type Op } from "@graphprotocol/grc-20";
import PostgreSQLClient, { TABLES, DB_ID } from "./src/postgres-client";
import path from 'path';


function printOps(ops: any) {
  const outputDir = path.join(__dirname, '');
  console.log("NUMBER OF OPS: ", ops.length);

  if (ops.length > 0) {
    // Get existing filenames in the directory
    const existingFiles = fs.readdirSync(outputDir);

    // Create output text
    const outputText = JSON.stringify(ops, null, 2);

    // Write to file
    const filename = `newly_created_ops_to_add_to_migration_data.txt`;
    const filePath = path.join(outputDir, filename);
    fs.writeFileSync(filePath, outputText);

    console.log(`OPS PRINTED to ${filename}`);
  } else {
    console.log("NO OPS TO PRINT");
  }
}

// Load and parse the file
//const filePath = "crypto_space_api_output.json";
//const data = JSON.parse(fs.readFileSync(filePath, "utf-8"));

let currentOps = readAllOpsFromFolder();

const cleanedOps = currentOps.map((op: any) => {
  const { spaceId, ...rest } = op;
  return rest;
});

printOps(cleanedOps)


