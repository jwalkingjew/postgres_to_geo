// To do
// - Do we want to add the project avatar as the avatar in the funding round entity?

import { TeableClient } from "./src/teable-client";
import { PROD_TABLE_IDS } from "./src/teable-meta";
import { getSpaces, searchEntity, filterOps, normalizeToUUID, normalizeToUUID_STRING, propertyToIdMap, GEO_IDS } from "./src/constants_v2";
import { processInvestment } from "./ethcc_post_investment";
import { processProject } from "./ethcc_post_project";
import * as fs from "fs";
import { publish } from "./src/publish";
import { testnetWalletAddress } from "./src/constants";
import { type Op } from "@graphprotocol/grc-20";

const teableClient = new TeableClient();
const currentOps: Array<Op> = [];

function printOps(ops: any) {
    console.log("NUMBER OF OPS: ", ops.length)
     if (ops.length > 0) {
        let outputText;
        // Convert operations to a readable JSON format
        outputText = JSON.stringify(ops, null, 2);
        // Write to a text file
        fs.writeFileSync(`ethcc_ops.txt`, outputText);
        console.log("OPS PRINTED")
    } else {
        console.log("NO OPS TO PRINT")
    }
}

async function publishOps(ops: any) {
    printOps(ops)
    if ((ops.length > 0) && (true)) {
        const iso = new Date().toISOString();
        let txHash;
        const spaces = await getSpaces(ops);

        for (const space of spaces) { 
            txHash = await publish({
                spaceId: space,
                author: testnetWalletAddress,
                editName: `Upload news stories ${iso}`,
                ops: await filterOps(ops, space), // An edit accepts an array of Ops
            }, "TESTNET");
    
            console.log(`Your transaction hash for ${space} is:`, txHash);
            console.log(iso);
            
            console.log(`Number of ops published in ${space}: `, (await filterOps(ops, space)).length)
        }   
        console.log(`Total ops: ${ops.length}`);
    } else {
        const spaces = await getSpaces(ops);
        console.log("Spaces", spaces);
        for (const space of spaces) {
            console.log(`Number of ops published in ${space}: `, (await filterOps(ops, space)).length)
            console.log(await filterOps(ops, space))
        }
    }
}

async function iterateInvestments({
  currentOps,
  teableClient,
  table_id,
  project_id,
}: {
  currentOps: Array<Op>;
  teableClient: any;
  table_id: string;
  project_id: string;
}): Promise<{
    ops: Op[];
}> {
    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let geoId: string;
    let filter_obj;
    const take = 100; // or your preferred page size
    let skip = 0;

    filter_obj = JSON.parse(`{"conjunction":"and","filterSet":[{"fieldId":"project","operator":"is","value":"${project_id}"}]}`);

    while (true) {
        const page = await teableClient.get_records(table_id, filter_obj, take, skip);
        
        if (!page || page.length === 0) break;

        const seen = new Set();
        const filtered = page.filter((item: any) => {
          const key = [
            item.fields.raise_amount,
            item.fields.funding_round,
            item.fields.fundraising_name,
            item.fields.project,
            //item.fields.source,
          ].join('|');
        
          if (seen.has(key)) {
            return false;
          } else {
            seen.add(key);
            return true;
          }
        });

        for (const record of filtered) {
            addOps = await processInvestment({
                currentOps: [...currentOps, ...ops],
                investmentId: record.id,
                teableClient: teableClient
            })
            ops.push(...addOps.ops)
        }

        skip += take;
    }

    return { ops };
}

const main = async () => {

    

    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let geoId: string;
    let filter_obj;
    filter_obj = JSON.parse(`{"conjunction":"or","filterSet":[{"conjunction":"and","filterSet":[{"fieldId":"geo_id","operator":"isNotEmpty","value":null},{"fieldId":"space_id","operator":"is","value":"SgjATMbm41LX6naizMqBVd"}]},{"conjunction":"and","filterSet":[{"fieldId":"geo_id","operator":"isEmpty","value":null}]}]}`);
  
  //const uniqueFundraisingNames = await getAllUniqueFundraisingNames(teableClient, PROD_TABLE_IDS.InvestmentRounds)
  //console.log(uniqueFundraisingNames.length)
  //console.log(uniqueFundraisingNames);

    const teableClient = new TeableClient();
    const take = 100; // or your preferred page size
    let skip = 0;
    const seen = new Set<string>();

    let counter = 0;

    while (true) {
        const page = await teableClient.get_records(PROD_TABLE_IDS.Projects, filter_obj, take, skip);
        
        if (!page || page.length === 0) break;

        for (const record of page) {
            counter = counter + 1;
            console.log(counter);
            addOps = await processProject({
                currentOps: ops,
                projectId: record.id, //"rectJ1FAdTdu8gSPJSa"
                teableClient: teableClient
            });
            ops.push(...addOps.ops)

            addOps = await iterateInvestments({
                currentOps: ops,
                teableClient: teableClient,
                table_id: PROD_TABLE_IDS.InvestmentRounds,
                project_id: record.id //"rectJ1FAdTdu8gSPJSa"
            });
            ops.push(...addOps.ops);

            if (counter >= 1000) {
                console.log("For the first 1000 projects, ops.length = ", ops.length)
                printOps(ops)
                return;
            }
        }
        //printOps(ops)
        //await publishOps(ops);
        skip += take;

    }
    printOps(ops)
};

console.time("main execution");
main();
console.timeEnd("main execution");