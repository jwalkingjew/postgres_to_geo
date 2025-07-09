import { Graph, Id, SystemIds, type Op } from '@graphprotocol/grc-20';
import * as fs from "fs";
import { publish } from './src/publish';
import { addSpace, filterOps, GEO_IDS, getSpaces, normalizeToUUID, normalizeToUUID_STRING, propertyToIdMap, propertyToTypeIdMap, readAllOpsFromFolder, testnetWalletAddress } from './src/constants_v2';
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
    const filename = `add_properties_to_types.txt`;
    const filePath = path.join(outputDir, filename);
    fs.writeFileSync(filePath, outputText);

    console.log(`OPS PRINTED to ${filename}`);
  } else {
    console.log("NO OPS TO PRINT");
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

const ops: Array<Op> = [];
let addOps;

//delete all relations to wrong entity
//recreate them to the correct entity
//delete values for wrong entity
//delete relations from wrong entity

//Seed round
let entity_to_remove = "9959eb50-b029-4a15-8557-b39318cbb91b";
let properties_to_remove = ["a126ca53-0c8e-48d5-b888-82c734c38935"];
let relations_to_remove = ["6dbddba2-6e37-4e40-b8a0-2621f6a73dfd"]

addOps = Graph.unsetEntityValues({
  id: entity_to_remove,
  properties: properties_to_remove
});
ops.push(...addOps.ops);

for (const relation of relations_to_remove) {
  addOps = Graph.deleteRelation({id: relation});
  ops.push(...addOps.ops);
}

addOps = Graph.createRelation({
  fromEntity: "9959eb50-b029-4a15-8557-b39318cbb91b",
  toEntity: "37d2167f-b64a-4b68-be26-55b3608050e7",
  type: "8f151ba4-de20-4e3c-9cb4-99ddf96f48f1",
  entityId: "db72a91f-93a0-4c61-b8e6-86c825311adb"
});
ops.push(...addOps.ops);

//printOps(await addSpace(ops, GEO_IDS.cryptoSpace))
await publishOps(await addSpace(ops, GEO_IDS.rootSpace))


