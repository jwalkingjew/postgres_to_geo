import fs from 'fs';
import path from 'path';
import { addSpace, filterOps, GEO_IDS, getSpaces, testnetWalletAddress } from './src/constants_v2';
import { publish } from './src/publish';
import { Graph, type Op } from '@graphprotocol/grc-20';

function readOpsFromFile(index: number): any {
  const filePath = path.join(__dirname, 'ethcc_testnet_ops_to_publish', `ethcc_ops_${index}.txt`);

  if (!fs.existsSync(filePath)) {
    console.error(`File ethcc_ops_${index}.txt does not exist`);
    return null;
  }

  const fileContents = fs.readFileSync(filePath, 'utf-8');

  try {
    const ops = JSON.parse(fileContents);
    console.log(`Read ${ops.length} ops from ethcc_ops_${index}.txt`);
    return ops;
  } catch (err) {
    console.error(`Failed to parse JSON from file ethcc_ops_${index}.txt`, err);
    return null;
  }
}
function printOps(ops: any) {
  const outputDir = path.join(__dirname, '');
  console.log("NUMBER OF OPS: ", ops.length);

  if (ops.length > 0) {
    // Get existing filenames in the directory
    const existingFiles = fs.readdirSync(outputDir);

    // Create output text
    const outputText = JSON.stringify(ops, null, 2);

    // Write to file
    const filename = `ethcc_ops_delete.txt`;
    const filePath = path.join(outputDir, filename);
    fs.writeFileSync(filePath, outputText);

    console.log(`OPS PRINTED to ${filename}`);
  } else {
    console.log("NO OPS TO PRINT");
  }
}

async function publishOps(ops: any) {
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

function getPropertyIdsFromUpdateOp(ops: any[], entityId: string): string[] {
  const updateOp = ops.find(op =>
    op.type === "UPDATE_ENTITY" && op.entity.id === entityId
  );

  if (!updateOp || !Array.isArray(updateOp.entity.values)) {
    return [];
  }

  return updateOp.entity.values
    .map((value: any) => value.property)
    .filter((propertyId: any) => propertyId !== undefined && propertyId !== null);
}


const del_ops: Array<Op> = [];
const currSpaceId = GEO_IDS.cryptoSpace;
let addOps;

const index_num = 48;
const ops = readOpsFromFile(index_num);

const del_ent_vals: Array<Op> = [];

const updateEntityIds = ops
  .filter((op: any) => op.type === "UPDATE_ENTITY")
  .map((op: any) => op.entity.id);

const uniqueEntityIds: any[] = [...new Set(updateEntityIds)];

for (const entityId of uniqueEntityIds) {
    addOps = Graph.unsetEntityValues({
        id: entityId,
        properties: getPropertyIdsFromUpdateOp(ops, entityId)
    })
    del_ops.push(...addOps.ops);
}

const createRelationIds = ops
  .filter((op: any) => op.type === "CREATE_RELATION")
  .map((op: any) => op.relation.id);

const uniqueRelationIds: any[] = [...new Set(createRelationIds)];

for (const relationId of uniqueRelationIds) {
    addOps = Graph.deleteRelation({id: relationId});
    del_ops.push(...addOps.ops);
}



//printOps(del_ops)
await publishOps(await addSpace(del_ops, currSpaceId))

