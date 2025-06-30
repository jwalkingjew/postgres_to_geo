//Import necessary libraries
import { Client } from 'pg';

import { v4 as uuidv4 } from 'uuid';
import * as fs from "fs";
import md5 from 'crypto-js/md5';
import {Id, Base58, SystemIds, Graph, Position, type Op} from "@graphprotocol/grc-20";

import { validate as uuidValidate } from 'uuid';

const mainnet_query_url = "https://hypergraph.up.railway.app/graphql";
//const testnet_query_url = "https://geo-conduit.up.railway.app/graphql";
const testnet_query_url = "https://hypergraph-v2-testnet.up.railway.app/graphql"
const QUERY_URL = testnet_query_url;


export const testnetWalletAddress = "0x84713663033dC5ba5699280728545df11e76BCC1";
export const mainnetWalletAddress = "0x0A77FD6b13d135426c25E605a6A4F39AF72fD967";

export const GEO_IDS = {
  coincarp: "2wrGpfxCX3sEL3gjUhG6or",

  rootSpace: "64ed9ffa-e7b3-40f6-ae99-fbf6112d10f8",
  cryptoSpace: "065d5e6f-0b3c-45b3-a5db-ace191e5a35c",
  cryptoEventsSpace: "d4cd9afa-3edf-4739-8537-ebb46da159f7",
  regionsSpace: "1f230cb3-145c-4e4b-b325-e85b0f8a212e",

  project_type_id: "9vk7Q3pz7US3s2KePFQrJT",
  types_property_id: "Jfmby78N4BCseZinBmdVov",

  //Investments types
  fundingRoundType: "JfHmjrWQ461P4stSiVQxWj",
  fundingStageType: "JSMtBBYEMrXNhcZ8iPTb4a",
  investorType: "7K28NfMNjj2RRi3xY735DN",

  //Space IDs
  cryptoNewsSpaceId: "BDuZwkjCg3nPWMDshoYtpS",
  //cryptoSpaceId: "065d5e6f-0b3c-45b3-a5db-ace191e5a35c",//"SgjATMbm41LX6naizMqBVd",

  sourceDBIdentifier: "CgLt3CoEzWmhPW3XGkakYa",
  propertiesSourced: "49frzgU1girWK2NNzXHJWn",
  relationsSourced: "5eCHqLU5t9DSkpLqnne252",

  draftTypeId: "5rtNCuvTUghFtMDhFYvfYB", 
  newsStoryOfTheDayTagId: "M8KF4D9sADRiug5RLhfGhY",
  newsStoryOfTheWeekTagId: "KwaMjDaWwvCX8uD938T7qW",

  blocksTypeId: "QYbjCM6NT9xmh2hFGsqpQX",

  bulletListView: "2KQ8CHTruSetFbi48nsHV3",

  improvementProposalTypeId: "6wya2r7xivwKiYSXuKgGSM",
  discussionLinkPropertyId: "7b8cbsM38h8PWXQ29isjdk", 
  statusPropertyId: "XHoA7MeFgHXGgCqn257s5F", 
  abstractPropertyId: "92PL1JTfCkuKDT4BLXsFX3", 
  networkPropertyId: "MuMLDVbHAmRjZQjhyk3HGx",

  xLinkPropertyId: "2eroVfdaXQEUw314r5hr35",
  websitePropertyId: "WVVjk5okbvLspwdY1iTmwp",
  rolesPropertyId: "JkzhbbrXFMfXN7sduMKQRp",
  authorRoleId: "WMc1G1C78zdVrigzFYBNBp", //NEED TO CREATE THIS
  publishedInPropertyId: "JEKLKQP5H8NQdVtvGaEFT6", //NEED TO CREATE THIS
  worksAtPropertyId: "U1uCAzXsRSTP4vFwo1JwJG",

  //News story IDs
  newsStoryTypeId: "VKPGYGnFuaoAASiAukCVCX",
  maintainersPropertyId: "Vtmojxf3rDL9VaAjG9T2NH",
  tagsPropertyId: "5d9VVey3wusmk98Uv3v5LM",
  tagTypeId: "UnP1LtXV3EhrhvRADFcMZK",

  //Source Property IDs
  articleTypeId: "M5uDP7nCw3nvfQPUryn9gx",
  postTypeId: "X7KuZJQewaCiCy9QV2vjyv",

  webURLId: "93stf6cgYvBsdPruRzq1KK",
  webArchiveURLId: "BTNv9aAFqAzDjQuf4u2fXK",
  publishDateId: "KPNjGaLx5dKofVhT6Dfw22",
  avatarPropertyId: "399xP4sGWSoepxeEnp3UdR",
  publisherPropertyId: "Lc4JrkpMUPhNstqs7mvnc5",
  publisherTypeId: "BGCj2JLjDjqUmGW6iZaANK",

  //Claim Property IDs
  claimTypeId: "KeG9eTM8NUYFMAjnsvF4Dg",
  newsEventTypeId: "QAdjgcq9nD7Gv98vn2vrDd",
  eventDatePropertyId: "BBA1894NztMD9dWyhiwcsU",
  quotesSupportingPropertyId: "quotesThatSupportClaims",

  //Quote property IDs
  quoteTypeId: "XGsAzMuCVXPtV8e6UfMLd",
  sourcesPropertyId: "A7NJF2WPh8VhmvbfVWiyLo",
  authorsPropertyId: "JzFpgguvcCaKhbQYPHsrNT",
  relatedPeoplePropertyId: "Cc3AZqRReWs3Zk2W5ALtyw",
  relatedProjectsPropertyId: "EcK9J1zwDzSQPTnBRcUg2A",
  relatedTopicsPropertyId: "GrojMdwbutDvrciUgcL2e4",
  topicTypeId: "Cj7JSjWKbcdgmUjcLWNR4V",
}

export const normalizeUrl = (url: string) =>
    url.endsWith('/') ? url.slice(0, -1) : url;

export function cleanText(input: string): string {
  // Remove invisible/control characters from the start and end
  return input
    .replace(/^[^\P{C}\P{Z}\S]+|[^\P{C}\P{Z}\S]+$/gu, '') // Strip non-printing characters at edges
    .trim(); // Also trim standard whitespace
}

export async function searchOps(ops: Array<Op>, property: string, propType: string, searchText: string, typeId: string | null = null) {
    
    let match;
    if (propType == "URL") {
        match = ops.find(op =>
            op.type === "UPDATE_ENTITY" &&
            Array.isArray(op.entity?.values) &&
            op.entity.values.some(
                (v: { property: string; value: string }) =>
                v.property === normalizeToUUID_STRING(property) &&
                normalizeUrl(v.value) === normalizeUrl(searchText)
            )
        );
    } else {
        match = ops.find(op =>
            op.type === "UPDATE_ENTITY" &&
            Array.isArray(op.entity?.values) &&
            op.entity.values.some(
                (v: { property: string; value: string }) =>
                v.property === normalizeToUUID_STRING(property) &&
                v.value?.toLowerCase() === searchText?.toLowerCase()
            )
        );
    }

    
    
    if (match) {
        if (typeId) {
            const matchType = ops.find(op =>
                op.type === "CREATE_RELATION" &&
                op.relation.fromEntity === match?.entity?.id &&
                op.relation.type === SystemIds.TYPES_PROPERTY &&
                op.relation.toEntity === normalizeToUUID_STRING(typeId)
            );
            if (matchType) {
                //console.log("Match found", match.entity.id)
                return match.entity.id
            } else {
                return null
            }

        } else {
            return match.entity.id;
        }
    } else {
        return null
    }
}

export async function hasBeenEdited(ops: Array<Op>, entityId: string): Promise<boolean> {
    
    let match;
    match = ops.find(op =>
        op.type === "UPDATE_ENTITY" &&
        op.entity.id === normalizeToUUID_STRING(entityId)
    );

    if (match) {
        return true;
    }

    match = ops.find(op =>
        op.type === "CREATE_RELATION" &&
        op.relation.fromEntity === normalizeToUUID_STRING(entityId)
    );

    if (match) {
        return true;
    } else {
        return false;
    }
}


async function fetchWithRetry(query: string, variables: any, retries = 3, delay = 200) {
    for (let i = 0; i < retries; i++) {
        const response = await fetch(QUERY_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ query, variables }),
        });

        if (response.ok) {
            return await response.json();
        }

        if (i < retries - 1) {
            // Optional: only retry on certain error statuses
            if (response.status === 502 || response.status === 503 || response.status === 504) {
                await new Promise(resolve => setTimeout(resolve, delay * (2 ** i))); // exponential backoff
            } else {
                break; // for other errors, don’t retry
            }
        } else {
            console.log("searchEntities");
            console.log(`SPACE: ${variables.space}; PROPERTY: ${variables.property}; searchText: ${variables.searchText}; typeId: ${variables.typeId}`);
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
    }
}

export async function searchEntities(space: string, property: string, searchText: string, typeId: string | null = null) {
    await new Promise(resolve => setTimeout(resolve, 200));
    let query;
    let variables;
    
    if (typeId) {
        query = `
            query GetEntities(
                $space: String!
                $property: String!
                $searchText: String!
                $typeId: String!
                $typesPropertyId: String!
            ) {
                entities(spaceId: $space, 
                    filter: { value: { property: $property, text: {is: $searchText} },
                              relations: { typeId: $typesPropertyId, toEntityId: $typeId }
                    }
                ) {
                    id
                    name
                }
            }
        `;

        variables = {
            space: normalizeToUUID_STRING(space),
            property: normalizeToUUID_STRING(property),
            searchText: cleanText(searchText),
            typeId: normalizeToUUID_STRING(typeId),
            typesPropertyId: SystemIds.TYPES_PROPERTY
        };
    } else {
        query = `
            query GetEntities(
                $space: String!
                $property: String!
                $searchText: String!
            ) {
                entities(spaceId: $space, 
                    filter: { value: { property: $property, text: {is: $searchText} } }
                ) {
                    id
                    name
                }
            }
        `;

        variables = {
            space: normalizeToUUID_STRING(space),
            property: normalizeToUUID_STRING(property),
            searchText: cleanText(searchText),
        };
    }

    const data = await fetchWithRetry(query, variables);
    
    if (data?.data?.entities?.length == 1) { //NOTE NEED TO HANDLE IF THERE ARE MANY RESULTS
        return data?.data?.entities?.[0]?.id;
    } else {
        if (data?.data?.entities?.length > 1) {
            console.error("DUPLICATE ENTITIES FOUND...")
            console.log(data?.data?.entities);
            return data?.data?.entities?.[0]?.id;
        }
        return null;
    }
}

export async function searchEntity(entityId: string, spaceId: string) {
    await new Promise(resolve => setTimeout(resolve, 200));
    let query;
    let variables;

    query = `
        query GetEntity(
            $entityId: String!
            $spaceId: String!
        ) {
            entity(id: $entityId, spaceId: $spaceId) {
                id
                name
                values {
                    spaceId
                    propertyId
                    value
                }
                relations {
                    id
                    spaceId
                    fromId
                    toId
                    typeId
                    entityId
                }
            }
        }
    `;

    variables = {
        entityId: normalizeToUUID_STRING(entityId),
        spaceId: normalizeToUUID_STRING(spaceId),
    };

    const data = await fetchWithRetry(query, variables);
    
    return data?.data?.entity;
}



export function isValid(id: string): boolean {
  if (id.length !== 22 && id.length !== 21) {
    return false;
  }

  try {
    const decoded = Base58.decodeBase58ToUUID(id);
    return uuidValidate(decoded);
  } catch (error) {
    return false;
  }
}

export function deterministicIdFromString(input: string): string {
    // Step 1: Hash input using MD5
    const hash = md5(input).toString(); // 32 hex chars
  
    // Step 2: Format into UUIDv4 style manually
    let uuid = [
      hash.substring(0, 8),
      hash.substring(8, 12),
      '4' + hash.substring(13, 16),            // Set version 4 (UUID v4)
      ((parseInt(hash.substring(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, '0') + hash.substring(18, 20), // Set variant
      hash.substring(20, 32)
    ].join('-');
  
    // Step 3: Remove dashes
    return uuid;//.replace(/-/g, '');
  }

export function normalizeToUUID_STRING(id: string): string {
    if (isUUID(id)) {
      return id;
    }
    //const base58Regex = /^[1-9A-HJ-NP-Za-km-z]{22}$/; // Common Base58 UUID format
    //const base58Regex = /^[1-9A-HJ-NP-Za-km-z]{21,22}$/;
  
    if (isValid(id)) {
      try {
        return Base58.decodeBase58ToUUID(id);
      } catch (e) {
        // Fall through if decoding fails
      }
    }
  
    return deterministicIdFromString(id);
  }

export function normalizeToUUID(id: string) {
    return Id.Id(normalizeToUUID_STRING(id))
  }

export function isUUID(str: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(str);
  }



export const propertyToIdMap: Record<string, string> = {
  //Project entity
  name: "LuBWqZAu6pz54eiJS5mLv8",
  description: "LA1DqP5v6QAdsgLPXGF3YA",
  logo: "399xP4sGWSoepxeEnp3UdR", // avatar property
  cover: "7YHk6qYkNDaAtNb8GwmysF",
  website_url: "WVVjk5okbvLspwdY1iTmwp",
  x_url: "2eroVfdaXQEUw314r5hr35",
  linkedin_url: "SRyePtjTYASwfq1kCjUaQf",
  date_founded: "97JK5WV4YeGeU3k5UtV5RX", //Date founded property
  

  //Investments relation entity
  funding_rounds: "A3FTmHJmJ2HipDShTeKiQ9", //Used on relation entity...

  //Funding round entity
  fundraising_name: "LuBWqZAu6pz54eiJS5mLv8",
  round_date: "BBA1894NztMD9dWyhiwcsU",
  raise_amount: "3mvgwJdxVbL4ASm2MHwWPG", //created incorrectly...
  funding_round: "Uy1bposi1eroNx7bFrWDwF", //Funding stage
  investor: "LCzhzbNhi4xVWuqXYiKNhC", //investors property
  invested_in: "K5zhVJwyzZwgnzeXXTz77z", //invested in property - renamed to investments
  project: "PHy9PS2KoC8hnsAzYQdz9k", //"K5zhVJwyzZwgnzeXXTz77z" - Investments: property // "PHy9PS2KoC8hnsAzYQdz9k", //Raised by: property

  //tag properties
  related_industries: 'WAsByt3z8T5Z71PgvqFGcm',
  related_services_or_products: '2YYDYuhjcfLGKKaTGB3Eim',
  related_asset_categories: 'UrHA7JUtaTy6emMcTm1ht8', // TODO - confirm edits accepted
  related_technologies: 'GHjnB8sdWYRTjZ1Z8hEpsw', // TODO - confirm edits accepted
  tags: '5d9VVey3wusmk98Uv3v5LM',
  
};

export const propertyToTypeIdMap: Record<string, string> = {
  //tag types
  related_industries: 'YA7mhzaafD2vnjekmcnLER',
  related_services_or_products: '4AsBUG91niU59HAevRNsbQ',
  related_asset_categories: 'YG3gRcykeAwG7VbinNL27j', // TODO - confirm edits accepted
  related_technologies: 'SAdaKzTJ37swD6ohJpFJE7', // TODO - confirm edits accepted
  tags: 'UnP1LtXV3EhrhvRADFcMZK',
  
};

const tagProperties = ['related_industries', 'related_services_or_products', 'related_asset_categories', 'related_technologies', 'tags']

export async function valuePropertyExistsOnGeo(spaceId: string, entityOnGeo: any, propertyId: string): Promise<boolean> {
    let geoProperties;

    if (entityOnGeo) {
        geoProperties = entityOnGeo?.values?.filter(
            (item) => 
                item.spaceId === spaceId &&
                item.propertyId === normalizeToUUID_STRING(propertyId)
        );

        if (geoProperties.length > 0) { //Note if it is greater than 1, we may be dealing with a multi space entity and I need to make sure I am in the correct space...
            return true;
        }
    }
    
    return false;
}

export async function relationPropertyExistsOnGeo(spaceId: string, entityOnGeo: any, propertyId: string): Promise<boolean> {
    let geoProperties = [];

    if (entityOnGeo) {
        geoProperties = entityOnGeo?.relations?.filter(
            (item) => 
                item.spaceId === spaceId &&
                item.typeId === normalizeToUUID_STRING(propertyId)
        );
        if (geoProperties.length > 0) { //Note if it is greater than 1, we may be dealing with a multi space entity and I need to make sure I am in the correct space...
            return true;
        }
    }
    
    return false;
}

export async function processNewRelation(currenOps: Array<Op>, spaceId: string, entityOnGeo: any, geoId: string, toEntityId: string, propertyId: string, position?: string, reset_position?: boolean, relationEntity?: string,): Promise<[Array<Op>, string]> {
    let geoProperties;
    const ops: Array<Op> = [];
    let addOps;

    if (!relationEntity) {
      relationEntity = Id.generate();
    }
    if (!position) {
      position = Position.generateBetween(null, null)
    }

    //Need to also search in the current ops whether relation exists...

    const match = currenOps.find(op =>
        op.type === "CREATE_RELATION" &&
        op.relation.fromEntity === normalizeToUUID_STRING(geoId) &&
        op.relation.type === normalizeToUUID_STRING(propertyId) &&
        op.relation.toEntity === normalizeToUUID_STRING(toEntityId)
    );
    if (match) {
        return [ops, match.relation.entity];
    }

    if (entityOnGeo) {
        geoProperties = entityOnGeo?.relations?.filter(
            (item) => 
                item.spaceId === spaceId &&
                item.typeId === normalizeToUUID_STRING(propertyId) &&
                item.toId === normalizeToUUID_STRING(toEntityId)
        );
        if (!geoProperties) {
            geoProperties = []
        }

        if (geoProperties.length == 0) {
            addOps = Graph.createRelation({
                toEntity: normalizeToUUID(toEntityId),
                fromEntity: normalizeToUUID(geoId),
                type: normalizeToUUID(propertyId),
                position: position,
                entityId: normalizeToUUID(relationEntity)
            });
            ops.push(...addOps.ops);
        } else {
            if ((reset_position) && (geoProperties.length == 1)) {
                console.error("WRITE CODE TO UPDATE RELATION POSITION")
                //addOps = Triple.make({
                //  entityId: geoProperties?.[0]?.entityId,
                //  attributeId: SystemIds.RELATION_INDEX,
                //  value: {
                //      type: "TEXT",
                //      value: position,
                //  },
                //});
                //ops.push(addOps);
            }  else if ((geoProperties.length > 1)) {
                console.error("DUPLICATE relations found on: ", geoId)
                for (let i = 1; i < geoProperties.length; i++) {
                    addOps = Graph.deleteRelation({id: geoProperties?.[i]?.id})
                    ops.push(...addOps.ops);
                }
                
                // Remove duplicates
            }
            relationEntity = geoProperties?.[0]?.entityId;
            if (!relationEntity) {
                relationEntity = "RELATION EXISTS - ERROR FINDING RELATION ENTITY"
                console.error(relationEntity)
                console.log(geoProperties)
                
            }
            return [ops, relationEntity];
        }
    } else {
        //console.log("From entity: ", normalizeToUUID(geoId))
        //console.log("To entity: ", normalizeToUUID(toEntityId))
        //console.log("Type: ", normalizeToUUID(propertyId))
        addOps = Graph.createRelation({
            toEntity: normalizeToUUID(toEntityId),
            fromEntity: normalizeToUUID(geoId),
            type: normalizeToUUID(propertyId),
            position: position,
            entityId: normalizeToUUID(relationEntity)
        });
        ops.push(...addOps.ops);
    }

    return [ops, relationEntity];
}


export async function addSpace(ops: Op | Op[], spaceId: string): Promise<Op | Op[]> {
  const addIfMissing = (op: Op): Op => {
    if (Array.isArray(op)) {
      throw new Error('Received array instead of Op in addIfMissing');
    }
    return 'spaceId' in op ? op : { ...op, spaceId };
  };

  if (Array.isArray(ops)) {
    return ops.map(addIfMissing);
  } else {
    return addIfMissing(ops);
  }
}

export async function filterOps(ops: Op | Op[], spaceId: string): Promise<Op | Op[] | null> {
  const clean = (op: Op): Op => {
    const { spaceId: _, ...rest } = op;
    return rest;
  };

  if (Array.isArray(ops)) {
    const filtered = ops.filter(op => op.spaceId === spaceId).map(clean);
    return filtered;
  } else {
    if (ops.spaceId === spaceId) {
      return clean(ops);
    } else {
      return null;
    }
  }
}


export async function getSpaces(ops: Op | Op[]): Promise<string[]> {
  const opsArray = Array.isArray(ops) ? ops : [ops];
  const spaceIds = opsArray
    .map(op => op.spaceId)
    .filter((id): id is string => typeof id === 'string');

  return Array.from(new Set(spaceIds));
}



export async function addSources(currentOps: Op[], entityId: string, sourceEntityId: string, propertiesSourced: string[], source_url?: string, source_id?: string, toEntity?: string) {
    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;

    const entityOnGeo = await searchEntity(entityId, currSpaceId);
    
    //Create relation from entity to source
    //Create relation entity that contains properties: source_url (web_url_property), Properties sourced

    addOps = await processNewRelation([...ops, ...currentOps], currSpaceId, entityOnGeo, normalizeToUUID(entityId), normalizeToUUID(sourceEntityId), normalizeToUUID(GEO_IDS.sourcesPropertyId));
    ops.push(...addOps[0]);

    const relationEntityId = addOps[1]

    const relEntityOnGeo = await searchEntity(relationEntityId, currSpaceId);

    const values = []
    if (source_url) {
        if (!(await valuePropertyExistsOnGeo(currSpaceId, relEntityOnGeo, normalizeToUUID(GEO_IDS.webURLId)))) {
            values.push({property: normalizeToUUID(GEO_IDS.webURLId), value: source_url})
        }
    }

    if (source_id) {
        if (!(await valuePropertyExistsOnGeo(currSpaceId, relEntityOnGeo, normalizeToUUID(GEO_IDS.sourceDBIdentifier)))) {
            values.push({property: normalizeToUUID(GEO_IDS.sourceDBIdentifier), value: source_id})
        }
    }

    if (values.length > 0) {
        addOps = Graph.createEntity({
            id: normalizeToUUID(relationEntityId),
            values: values
        })
        ops.push(...addOps.ops);
    }

    for (const property of propertiesSourced) {
        addOps = await processNewRelation([...ops, ...currentOps], currSpaceId, relEntityOnGeo, normalizeToUUID(relationEntityId), normalizeToUUID(property), normalizeToUUID(GEO_IDS.propertiesSourced));
        ops.push(...addOps[0]);

        if (toEntity) {
            let propSourcedRelEntityOnGeo = await searchEntity(addOps[1], currSpaceId);
            addOps = await processNewRelation([...ops, ...currentOps], currSpaceId, propSourcedRelEntityOnGeo, normalizeToUUID(addOps[1]), normalizeToUUID(toEntity), normalizeToUUID(GEO_IDS.relationsSourced));
            ops.push(...addOps[0]);
        }
    }

    return await addSpace(ops, currSpaceId);
}

