import { Graph, SystemIds, type Op, type ValueDataType } from '@graphprotocol/grc-20';
import * as fs from "fs";
import { publish } from './src/publish.ts';
import { addSpace, filterOps, GEO_IDS, getSpaces, normalizeToUUID, propertyToIdMap, readAllOpsFromFolder, renderableTypeToIdMap, testnetWalletAddress, typeToIdMap } from './src/constants_v2.ts';
import { printOps, publishOps } from "./src/functions.ts";
import path from "path";
import prettier from "prettier"; // optional, for nice formatting

async function updatePropertyMap(newEntries: Record<string, string>) {
  // merge new entries into the in-memory map
  Object.assign(propertyToIdMap, newEntries);

  // get the constants file path
  const constantsPath = path.resolve("./src/constants_v2.ts");

  // regenerate the file content
  const fileContent = `
export const propertyToIdMap: Record<string, string> = ${JSON.stringify(propertyToIdMap, null, 2)};
`;

  // optionally prettify
  const formatted = await prettier.format(fileContent, { parser: "typescript" });

  // write back to file
  fs.writeFileSync(constantsPath, formatted);
  console.log("✅ Updated propertyToIdMap in constants_v2.ts");
}

const ops: Array<Op> = [];
let addOps;
const usd_id = "0d4d1b02-a9e8-4982-92c4-99b1d22cd430";
const unit_property = "11b06581-20d3-41ea-b570-2ef4ee0a4ffd";

//Add relation entity types and make sure to map out all necessary relation entities with required properties
const properties = [
  //{ name: "Podcast", type: "RELATION" as const, relationValueTypes: ["podcast"], renderableType: null },
  //{ name: "Listen on", type: "RELATION" as const, relationValueTypes: ["project"], renderableType: null },
  //{ name: "Hosts", type: "RELATION" as const, relationValueTypes: ["person"], renderableType: null },
  //{ name: "Guests", type: "RELATION" as const, relationValueTypes: ["person"], renderableType: null },
  //{ name: "Contributors", type: "RELATION" as const, relationValueTypes: ["person"], renderableType: null },
  //{ name: "Episode number", type: "NUMBER" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Air date", type: "TIME" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Audio URL", type: "STRING" as const, relationValueTypes: [], renderableType: "url" },
  //{ name: "Duration", type: "NUMBER" as const, relationValueTypes: [], renderableType: null },
  //{ name: "RSS Feed URL", type: "STRING" as const, relationValueTypes: [], renderableType: "url" },
  //{ name: "Explicit", type: "BOOLEAN" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Topics", type: "RELATION" as const, relationValueTypes: ['topic'], renderableType: null }
  //PODCAST SHOULD BE PUBLISHED AFTER PODCAST TYPE IS CREATED
  //{ name: "Source database key", type: "STRING" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Attributed to", type: "RELATION" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Targets", type: "RELATION" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Start offset", type: "NUMBER" as const, relationValueTypes: [], renderableType: null },
  //{ name: "End offset", type: "NUMBER" as const, relationValueTypes: [], renderableType: null },
  //{ name: "Notable quotes", type: "RELATION" as const, relationValueTypes: ['quote'], renderableType: null },
  //{ name: "Notable claims", type: "RELATION" as const, relationValueTypes: ['claim'], renderableType: null },
  { name: "Broader topics", type: "RELATION" as const, relationValueTypes: ['topic'], renderableType: null },
  { name: "Subtopics", type: "RELATION" as const, relationValueTypes: ['topic'], renderableType: null },

];

const types = [
  //{ name: "Podcast", properties: ['name', 'avatar', 'description', 'date_founded', 'explicit', 'listen_on', 'hosts', 'topics', 'web_url', 'x_url', 'sources'] },
  //{ name: "Episode", properties: ['name', 'avatar', 'description', 'air_date', 'podcast', 'episode_number', 'listen_on', 'hosts', 'guests', 'contributors', 'notable_quotes', 'notable_claims', 'topics', 'sources'] },
  //{ name: "Podcast appearance", properties: ['roles'] },
  //{ name: "Selector", properties: ['start_offset', 'end_offset'] },

];

for (const type of types) { 
    const mappedProperties = type.properties
        .map(propName => propertyToIdMap[propName])    // get the ID from the map
        .filter(Boolean)                               // remove any undefined if a property is missing
        .map(normalizeToUUID);                         // run your transforming function
    
    addOps = Graph.createType({
        name: type.name,
        properties: mappedProperties
    })
    ops.push(...addOps.ops)

    console.log(`${type.name}: ${addOps.id}`)
}


for (const property of properties) {
    const mappedTypes = property.relationValueTypes
        ?.map(typeName => typeToIdMap[typeName])   // get the ID from the map
        .filter(Boolean)                               // remove any undefined if a property is missing
        .map(normalizeToUUID);                    // run your transforming function
    
    addOps = Graph.createProperty({
        name: property.name,
        dataType: property.type,
        ...(mappedTypes && mappedTypes.length > 0 && { relationValueTypes: mappedTypes })
    });
    ops.push(...addOps.ops)
    console.log(`${property.name}: ${addOps.id}`)

    if (property.renderableType) {
        addOps = Graph.createRelation({
            type: propertyToIdMap['renderable_type'],
            fromEntity: addOps.id,
            toEntity: renderableTypeToIdMap[property.renderableType]
        })
        ops.push(...addOps.ops)
    }
    
    
}

//console.log(await addSpace(ops, GEO_IDS.cryptoSpace))
await publishOps(await addSpace(ops, GEO_IDS.podcastsSpace))

