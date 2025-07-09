import { Graph, type Op } from '@graphprotocol/grc-20';
import * as fs from "fs";
import { publish } from './src/publish';
import { addSpace, filterOps, GEO_IDS, getSpaces, normalizeToUUID, propertyToIdMap, propertyToTypeIdMap, readAllOpsFromFolder, testnetWalletAddress } from './src/constants_v2';
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
    const filename = `Properties input.txt`;
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

const ops: Array<Op> = [];
let addOps;

addOps = Graph.createProperty({
    id: normalizeToUUID(propertyToIdMap['wallet_address']),
    name: "Wallet address",
    dataType: "TEXT"
})
ops.push(...addOps.ops)

addOps = Graph.createProperty({
    id: normalizeToUUID(propertyToIdMap['pricing_models']),
    name: "Pricing models",
    dataType: "RELATION"
})
ops.push(...addOps.ops)

addOps = Graph.createProperty({
    id: normalizeToUUID(propertyToIdMap['active_unique_wallets']),
    name: "Active unique wallets",
    dataType: "NUMBER"
})
ops.push(...addOps.ops)

addOps = Graph.createType({
    id: normalizeToUUID(GEO_IDS.appType),
    name: "App",
    properties: [
        normalizeToUUID(propertyToIdMap['logo']),
        normalizeToUUID(propertyToIdMap['website_url']),
        normalizeToUUID(propertyToIdMap['x_url']),
        normalizeToUUID(propertyToIdMap['github_url']),
        normalizeToUUID(propertyToIdMap['discord_url']),
        normalizeToUUID(propertyToIdMap['linkedin_url']),
        normalizeToUUID(propertyToIdMap['telegram_url']),
        normalizeToUUID(propertyToIdMap['medium_url']),
        normalizeToUUID(propertyToIdMap['pricing_models']),
        normalizeToUUID(propertyToIdMap['active_unique_wallets']),
        normalizeToUUID(propertyToIdMap['related_technologies']),
        normalizeToUUID(propertyToIdMap['related_asset_categories']),
        normalizeToUUID(propertyToIdMap['related_services_or_products']),
        normalizeToUUID(propertyToIdMap['related_ecosystems']),
        normalizeToUUID(propertyToIdMap['related_industries']),
        normalizeToUUID(propertyToIdMap['tags']),
    ]
});
ops.push(...addOps.ops)

addOps = Graph.createType({
    id: normalizeToUUID(propertyToTypeIdMap['pricing_models']),//propertyToTypeIdMap
    name: "Pricing model",
});
ops.push(...addOps.ops)



printOps(await addSpace(ops, GEO_IDS.cryptoSpace))
//await publishOps(await addSpace(ops, GEO_IDS.cryptoSpace))