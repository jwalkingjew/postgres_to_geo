//Import necessary libraries
import { v4 as uuidv4 } from 'uuid';
import * as fs from "fs";
import md5 from 'crypto-js/md5.js';
import {Id, Base58, SystemIds, Graph, Position, type Op, IdUtils} from "@graphprotocol/grc-20";
import dotenv from "dotenv";
import { validate as uuidValidate } from 'uuid';


export const testnetWalletAddress = process.env.W_ADDRESS;
export const mainnetWalletAddress = process.env.SW_ADDRESS;

export const GEO_IDS = {
  coincarp: "2wrGpfxCX3sEL3gjUhG6or",

  appType: "JNCkB5MTz1gmQVHP8BLirw",
  podchaserEntity: "edd1ef73-ec81-4c5f-a17b-749d95699f48",

  podcastsSpace: "530a70d9-d5ee-410a-850a-cf70e4be2ee5",
  rootSpace: "29c97d29-1c9a-41f1-a466-8a713764bc27",
  cryptoSpace: "b2565802-3118-47be-91f2-e59170735bac",
  cryptoEventsSpace: "dabe3133-4334-47a0-85c5-f965a3a94d4c",
  regionsSpace: "aea9f05a-2797-4e7e-aeae-5059ada3b56b",

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

export async function searchOps({
  ops,
  property,
  propType,
  searchText,
  typeId
}: {
  ops: Array<Op>;
  property: string;
  propType: string;
  searchText?: string;
  typeId?: string;
}) {
    if (!searchText) {
      return null;
    }
    let match;
    if (propType == "URL") {
        match = ops.find(op =>
            op.type === "UPDATE_ENTITY" &&
            Array.isArray(op.entity?.values) &&
            op.entity.values.some(
                (v: { property: string; value: string }) =>
                v.property == normalizeToUUID_STRING(property) &&
                normalizeUrl(v.value) == normalizeUrl(searchText)
            )
        );
    } else {
        match = ops.find(op =>
            op.type === "UPDATE_ENTITY" &&
            Array.isArray(op.entity?.values) &&
            op.entity.values.some(
                (v: { property: string; value: string }) =>
                v.property == normalizeToUUID_STRING(property) &&
                String(v.value)?.toLowerCase() == searchText?.toLowerCase()
            )
        );
    }

    
    
    if (match) {
        if (typeId) {
            const matchType = ops.find(op =>
                op.type == "CREATE_RELATION" &&
                op.relation.fromEntity == match?.entity?.id &&
                op.relation.type == SystemIds.TYPES_PROPERTY &&
                op.relation.toEntity == normalizeToUUID_STRING(typeId)
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

// --- Normalize UUID Functions

export function isUUID(str: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(str);
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

export function normalizeToUUID(id: string): Id {
    return Id(normalizeToUUID_STRING(id))
  }

export const propertyToIdMap: Record<string, string> = {
  name: "LuBWqZAu6pz54eiJS5mLv8",
  description: "LA1DqP5v6QAdsgLPXGF3YA",
  avatar: "399xP4sGWSoepxeEnp3UdR", // avatar property
  cover: "7YHk6qYkNDaAtNb8GwmysF",
  types: "Jfmby78N4BCseZinBmdVov",
  web_url: "WVVjk5okbvLspwdY1iTmwp",
  x_url: "2eroVfdaXQEUw314r5hr35",
  date_founded: "97JK5WV4YeGeU3k5UtV5RX", //Date founded property
  episode_number: "54abe3ac-f2ac-416a-933d-8357831dff70",
  listen_on: "71931b5f-1d6a-462e-81d9-5b8e85fb5c4b",
  podcast: "09ed1fd1-aced-469e-98d5-f036e6aa29c8",
  hosts: "5e3cc744-6e2f-4393-b1d6-d1ba577a2082",
  guests: "efb29e05-c7d7-4b74-a349-61f63cabf5ae",
  air_date: "253a0604-c129-4941-a4ad-07284971666b",
  sources: "A7NJF2WPh8VhmvbfVWiyLo",
  source_db_identifier: "CgLt3CoEzWmhPW3XGkakYa",
  topics: "458fbc07-0dbf-4c92-8f57-16f3fdde7c32",
  audio_url: "b5e70601-c985-4135-a5a0-7990b238a676",
  duration: "fc52bf99-471b-42e0-8635-99361b6bf83f",
  rss_feed_url: "4dd1a486-c1ad-48c6-b261-e4c8edf7ac65",
  explicit: "4dd1a486-c1ad-48c6-b261-e4c8edf7ac65",
  contributors: "c25ef1c6-8ba2-42c0-98cf-e2d2e052039d",
  roles: "8fcfe5ef-3d91-47bd-8322-3830a998d26b",
  renderable_type: "2316bbe1-c76f-4635-83f2-3e03b4f1fe46",
  source_db_key: "d1fa97b3-2ab4-4f18-bd5a-91868a63a392",
  supporting_quotes: "f9eeaf9d-9eb7-41b1-ac5d-257c6e82e526",
  attributed_to: "4c537c03-24b2-44ce-ba44-ee9e0bf406bb",
  text_blocks: "beaba5cb-a677-41a8-b353-77030613fc70",
  data_blocks: "beaba5cb-a677-41a8-b353-77030613fc70",
  tabs: "4d9cba1c-4766-4698-81cd-3273891a018b",
  data_source_type: "1f69cc98-80d4-44ab-ad49-3df6a7b15ee4",
  filter: "14a46854-bfd1-4b18-8215-2785c2dab9f3",
  markdown_content: "e3e363d1-dd29-4ccb-8e6f-f3b76d99bc33",
  notable_quotes: "dc637db0-3066-403c-a021-1d9eeea02d61",
  notable_claims: "4b298933-92e8-4b91-ac40-73d0da7834f3",
  targets: "b08235e4-725f-450b-899f-913b915ff58e",
  start_offset: "d2ac07e6-ea44-49b8-b1bd-d2984ebdf5c6",
  end_offset: "e449e3c7-c25b-4182-b4ce-d941b64b7b79",
  broader_topics: "db8481b9-ecc5-4a54-8073-dae3b6f5a8f6",
  subtopics: "a093cd2e-09fa-4c91-a1c0-d0bdcc68b439",
};

export const typeToIdMap: Record<string, string> = {
  podcast: "69732974-c632-490d-81a3-12ea567b2a8e",
  episode: "11feb0f9-fb3b-442c-818a-b5e97ffde26a",
  person: "7ed45f2b-c48b-419e-8e46-64d5ff680b0d",
  project: "484a18c5-030a-499c-b0f2-ef588ff16d50",
  source: "706779bf-5377-44a6-8694-ea06cf87a3a2",
  topic: "5ef5a586-0f27-4d8e-8f6c-59ae5b3e89e2",
  role: "e4e366e9-d555-4b68-92bf-7358e824afd2",
  podcast_appearance: "53841c11-19c6-473d-a093-b60968149f60",
  claim: "96f859ef-a1ca-4b22-9372-c86ad58b694b",
  quote: "043a171c-6918-4dc3-a7db-b8471ca6fcc2",
  text_block: "76474f2f-0089-4e77-a041-0b39fb17d0bf",
  data_block: "b8803a86-65de-412b-bb35-7e0c84adf473",
  page: "480e3fc2-67f3-4993-85fb-acdf4ddeaa6b",
  selector: "75e2a120-d283-4d27-a3bb-6e8e748a30c2",
};

export const renderableTypeToIdMap: Record<string, string> = {
  url: "283127c9-6142-4684-92ed-90b0ebc7f29a",
};



export const propertyToIdMap_for_investment_rounds: Record<string, string> = {
  //Project entity
  name: "LuBWqZAu6pz54eiJS5mLv8",
  description: "LA1DqP5v6QAdsgLPXGF3YA",
  logo: "399xP4sGWSoepxeEnp3UdR", // avatar property
  cover: "7YHk6qYkNDaAtNb8GwmysF",
  website_url: "WVVjk5okbvLspwdY1iTmwp",
  x_url: "2eroVfdaXQEUw314r5hr35",
  linkedin_url: "SRyePtjTYASwfq1kCjUaQf",
  date_founded: "97JK5WV4YeGeU3k5UtV5RX", //Date founded property
  telegram_url: "WepmF7ZjERNgYgPnJbKXve",
  medium_url: "CD9kCdEfrbpESXQMVJ7Wx3",
  discord_url: "NHUNHCVKpRjCYcbry57a9F",
  github_url: "LdGTXNrzLfyqm3HGQoefsZ",
  wallet_address: "A2txpy2v1ZQWQNxfEYLKPU",
  x_followers: "eb1a5120-90b6-4eb4-a8d5-338f29744bea",

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

  //tag / relation properties
  related_industries: 'WAsByt3z8T5Z71PgvqFGcm',
  related_services_or_products: '2YYDYuhjcfLGKKaTGB3Eim',
  related_asset_categories: 'UrHA7JUtaTy6emMcTm1ht8', // TODO - confirm edits accepted
  related_technologies: 'GHjnB8sdWYRTjZ1Z8hEpsw', // TODO - confirm edits accepted
  tags: '5d9VVey3wusmk98Uv3v5LM',
  related_assets: "6JwPE1KYoghpUGGzATkh46",
  related_platforms: "MuMLDVbHAmRjZQjhyk3HGx", // Network property
  platform_id: "MuMLDVbHAmRjZQjhyk3HGx", // Network property

  //Asset properties
  symbol: "NMCZXJNatQS59U31sZKmMn",
  max_supply: "7HuDtX2iJppMcWuZCEyPGx",
  token_address: "Bi28i5Xf6kjPyqP6svNgdX",
  price_usd: "P9kdgA83GGv7RwSJfQmrDp",
  percent_change_30d: "YFanfTCQ5QewQoybgZ22NX",
  percent_change_60d: "4v4oNCKUPRahPgVvYtfCTn",
  market_cap_usd: "VZaBNbnHbDMbKCjVswHXP5",
  tvl_usd: "5sSnsLsQRsvPsBFSXUFdcz",
  created_at: "JWL5DRgGpwY7NtBruPaGiE", // last_updated column (should change the name of this to date_updated...)
  related_ecosystems: "KJTYzcJDKmTDE4cnCshkZG",
  rootdata_x_influence_index: "Rugt3bTuHEp5MHoQNQCQ5C",
  pricing_models: "3eUu3TnbLD2xTT6y3rFqnm",
  active_unique_wallets: "SwcANCHcxXtes5k6qGAJzU",

  active_unique_wallets_24h: "AREigQLBNZT4U9VdNNPqqj",
  active_unique_wallets_7d: "C2BBqdKnR1RBQpTCgXpXP1",
  active_unique_wallets_30d: "UKz8nzNpDUz7hxYm6UYiWQ",

  transaction_count_24h: "W8B3v8vDA78VpNZnrzyUgL",
  transaction_count_7d: "9NJaNkCSY2AmL1VNEV4jC7",
  transaction_count_30d: "N3JErurFymmU2iMtxYpwHH",

  total_volume_24h: "Esu4cdFwbtDrC4i1soaEXe",
  total_volume_7d: "EBiqRfZFADfSwtb5ZdyvTk",
  total_volume_30d: "RpxusCGoSJC7eVSmX6XGW9",

  total_balance_24h: "Qfe1piarLYi6R4JgNcHZ2Z",
  total_balance_7d: "RPY7YoBWg1JHb8UmwZhXwJ",
  total_balance_30d: "HQBtmHqcGW7mhEoLXPKU4q",

  related_apps: "2kooB8MTNHXqBgFc6bKwjJ",
  related_projects: "EcK9J1zwDzSQPTnBRcUg2A",

};


export const propertyToTypeIdMap: Record<string, string> = {
  //tag / relation types
  related_industries: 'YA7mhzaafD2vnjekmcnLER', // Industry type
  related_services_or_products: '4AsBUG91niU59HAevRNsbQ', // Service type
  related_asset_categories: 'YG3gRcykeAwG7VbinNL27j', // TODO - confirm edits accepted
  related_technologies: 'SAdaKzTJ37swD6ohJpFJE7', // TODO - confirm edits accepted
  tags: 'UnP1LtXV3EhrhvRADFcMZK', // Tag type
  related_assets: "XgZFStBQiTP8a7ftKrT1FQ", // Asset type
  related_platforms: "YCLXoVZho6C4S51g4AbF3C", // Network type
  platform_id: "YCLXoVZho6C4S51g4AbF3C", // Network type
  related_ecosystems: "YCLXoVZho6C4S51g4AbF3C", // Network type
  pricing_models: "TGwxvFomquuYykMe3s536a",
};

export function valuePropertyExistsOnGeo(spaceId: string, entityOnGeo: any, propertyId: string): boolean {
    let geoProperties;

    if (entityOnGeo) {
        geoProperties = entityOnGeo?.values?.filter(
            (item: any) => 
                item.spaceId === normalizeToUUID_STRING(spaceId) &&
                item.propertyId === normalizeToUUID_STRING(propertyId)
        );

        if (geoProperties.length > 0) { //Note if it is greater than 1, we may be dealing with a multi space entity and I need to make sure I am in the correct space...
            return true;
        }
    }
    
    return false;
}

export function relationPropertyExistsOnGeo(spaceId: string, entityOnGeo: any, propertyId: string): boolean {
    let geoProperties = [];

    if (entityOnGeo) {
        geoProperties = entityOnGeo?.relations?.filter(
            (item: any) => 
                item.spaceId === normalizeToUUID_STRING(spaceId) &&
                item.typeId === normalizeToUUID_STRING(propertyId)
        );
        if (geoProperties.length > 0) { //Not true bc I am filtering by spaceId -> Note if it is greater than 1, we may be dealing with a multi space entity and I need to make sure I am in the correct space...
            return true;
        }
    }
    
    return false;
}

//export async function processNewRelation(currenOps: Array<Op>, spaceId: string, entityOnGeo: any, geoId: string, toEntityId: string, propertyId: string, position?: string, reset_position?: boolean, relationEntity?: string,): Promise<[Array<Op>, string]> {
export function processNewRelation({
  currenOps,
  spaceId,
  entityOnGeo,
  fromEntityId,
  toEntityId,
  propertyId,
  //position,
  last_position,
  //reset_position,
  relationEntity
}: {
  currenOps: Array<Op>;
  spaceId: string;
  entityOnGeo?: any;
  fromEntityId: string;
  toEntityId: string;
  propertyId: string;
  //position?: string;
  last_position?: string;
  //reset_position?: boolean;
  relationEntity?: string;
}): { ops: Array<Op>; relationEntityId: string; position: string;} {
  //TODO SHOULD I INSTEAD BE SENDING THE LAST POSITION IN AND THEN I COULD COMPARE WHETHER I NEED TO RESET THIS ONE?

    let geoProperties;
    const ops: Array<Op> = [];
    let addOps;
    let position;

    if (!relationEntity) {
      relationEntity = IdUtils.generate();
    }
    if (last_position) {
      position = Position.generateBetween(last_position, null)
    } else {
      position = Position.generateBetween(null, null)
    }

    // Search in the current ops whether relation exists...
    const match = currenOps.find(op =>
        op.type === "CREATE_RELATION" &&
        op.relation.fromEntity === normalizeToUUID_STRING(fromEntityId) &&
        op.relation.type === normalizeToUUID_STRING(propertyId) &&
        op.relation.toEntity === normalizeToUUID_STRING(toEntityId)
    );
    if (match) {
        return { ops: ops, relationEntityId: match.relation.entity, position: match.relation.position };
    }
 
    const args = arguments[0];
    if (!("entityOnGeo" in args)) {
      console.log("SEARCHING UNDEFINED")
        //entityOnGeo = await searchEntity({
        //    entityId: fromEntityId,
        //    spaceId: spaceId
        //});
    }
    if (entityOnGeo) {
        
        geoProperties = entityOnGeo?.relations?.filter(
            (item) => 
                item.spaceId == spaceId &&
                item.typeId == normalizeToUUID_STRING(propertyId) &&
                item.toEntityId == normalizeToUUID_STRING(toEntityId)
        );
        if (!geoProperties) {
            geoProperties = []
        }

        if (geoProperties.length == 0) {
            addOps = Graph.createRelation({
                toEntity: normalizeToUUID(toEntityId),
                fromEntity: normalizeToUUID(fromEntityId),
                type: normalizeToUUID(propertyId),
                position: position,
                entityId: normalizeToUUID(relationEntity)
            });
            ops.push(...addOps.ops);
        } else {
            if ((last_position) && (Position.compare(geoProperties?.[0]?.position, last_position) != 1)){
                console.error("WRITE CODE TO UPDATE RELATION POSITION")
                
                //addOps = Graph.createRelation({
                //  id: geoProperties?.[0]?.id,
                //  position: position,
                //})

                //Update position of relation to correctly set one.
                //geoProperties?.[0]?.id
            } 
            if ((geoProperties.length > 1)) {
                console.error("DUPLICATE relations found on: ", fromEntityId)
                for (let i = 1; i < geoProperties.length; i++) {
                    addOps = Graph.deleteRelation({id: geoProperties?.[i]?.id})
                    ops.push(...addOps.ops);
                    console.log("DUPLICATES REMOVED")
                }
            }
            relationEntity = geoProperties?.[0]?.entityId;
            if (!relationEntity) {
                relationEntity = "RELATION EXISTS - ERROR FINDING RELATION ENTITY"
                console.error(relationEntity)
                console.log(geoProperties)
                
            }
            return { ops: ops, relationEntityId: relationEntity, position: geoProperties?.[0]?.position };
        }
    } else {
        //console.log("From entity: ", normalizeToUUID(fromEntityId))
        //console.log("To entity: ", normalizeToUUID(toEntityId))
        //console.log("Type: ", normalizeToUUID(propertyId))
        addOps = Graph.createRelation({
            toEntity: normalizeToUUID(toEntityId),
            fromEntity: normalizeToUUID(fromEntityId),
            type: normalizeToUUID(propertyId),
            position: position,
            entityId: normalizeToUUID(relationEntity)
        });
        ops.push(...addOps.ops);
    }

    return { ops: ops, relationEntityId: relationEntity, position: position };
}

export async function processNewRelation_v1({
  currenOps,
  spaceId,
  entityOnGeo,
  fromEntityId,
  toEntityId,
  propertyId,
  //position,
  last_position,
  //reset_position,
  relationEntity
}: {
  currenOps: Array<Op>;
  spaceId: string;
  entityOnGeo?: any;
  fromEntityId: string;
  toEntityId: string;
  propertyId: string;
  //position?: string;
  last_position?: string;
  //reset_position?: boolean;
  relationEntity?: string;
}): Promise<{ ops: Array<Op>; relationEntityId: string; position: string;}> {
  //TODO SHOULD I INSTEAD BE SENDING THE LAST POSITION IN AND THEN I COULD COMPARE WHETHER I NEED TO RESET THIS ONE?

    let geoProperties;
    const ops: Array<Op> = [];
    let addOps;
    let position;

    if (!relationEntity) {
      relationEntity = IdUtils.generate();
    }
    if (last_position) {
      position = Position.generateBetween(last_position, null)
    } else {
      position = Position.generateBetween(null, null)
    }

    // Search in the current ops whether relation exists...
    const match = currenOps.find(op =>
        op.type === "CREATE_RELATION" &&
        op.relation.fromEntity === normalizeToUUID_STRING(fromEntityId) &&
        op.relation.type === normalizeToUUID_STRING(propertyId) &&
        op.relation.toEntity === normalizeToUUID_STRING(toEntityId)
    );
    if (match) {
        return { ops: ops, relationEntityId: match.relation.entity, position: match.relation.position };
    }
 
    const args = arguments[0];
    if (!("entityOnGeo" in args)) {
      console.log("SEARCHING UNDEFINED")
        entityOnGeo = await searchEntity({
            entityId: fromEntityId,
            spaceId: spaceId
        });
    }
    if (entityOnGeo) {
        
        geoProperties = entityOnGeo?.relations?.filter(
            (item) => 
                item.spaceId === spaceId &&
                item.typeId === normalizeToUUID_STRING(propertyId) &&
                item.toEntityId === normalizeToUUID_STRING(toEntityId)
        );
        if (!geoProperties) {
            geoProperties = []
        }

        if (geoProperties.length == 0) {
            addOps = Graph.createRelation({
                toEntity: normalizeToUUID(toEntityId),
                fromEntity: normalizeToUUID(fromEntityId),
                type: normalizeToUUID(propertyId),
                position: position,
                entityId: normalizeToUUID(relationEntity)
            });
            ops.push(...addOps.ops);
        } else {
            if ((last_position) && (Position.compare(geoProperties?.[0]?.position, last_position) != 1)){
                console.error("WRITE CODE TO UPDATE RELATION POSITION")
                
                //addOps = Graph.createRelation({
                //  id: geoProperties?.[0]?.id,
                //  position: position,
                //})

                //Update position of relation to correctly set one.
                //geoProperties?.[0]?.id
            } 
            if ((geoProperties.length > 1)) {
                console.error("DUPLICATE relations found on: ", fromEntityId)
                for (let i = 1; i < geoProperties.length; i++) {
                    addOps = Graph.deleteRelation({id: geoProperties?.[i]?.id})
                    ops.push(...addOps.ops);
                    console.log("DUPLICATES REMOVED")
                }
            }
            relationEntity = geoProperties?.[0]?.entityId;
            if (!relationEntity) {
                relationEntity = "RELATION EXISTS - ERROR FINDING RELATION ENTITY"
                console.error(relationEntity)
                console.log(geoProperties)
                
            }
            return { ops: ops, relationEntityId: relationEntity, position: geoProperties?.[0]?.position };
        }
    } else {
        //console.log("From entity: ", normalizeToUUID(fromEntityId))
        //console.log("To entity: ", normalizeToUUID(toEntityId))
        //console.log("Type: ", normalizeToUUID(propertyId))
        addOps = Graph.createRelation({
            toEntity: normalizeToUUID(toEntityId),
            fromEntity: normalizeToUUID(fromEntityId),
            type: normalizeToUUID(propertyId),
            position: position,
            entityId: normalizeToUUID(relationEntity)
        });
        ops.push(...addOps.ops);
    }

    return { ops: ops, relationEntityId: relationEntity, position: position };
}


export async function addSpace(ops: Op | Op[], spaceId: string): Promise<Op[]> {
  const addIfMissing = (op: Op): Op => {
    if (Array.isArray(op)) {
      throw new Error('Received array instead of Op in addIfMissing');
    }
    return 'spaceId' in op ? op : { ...op, spaceId };
  };

  return Array.isArray(ops)
    ? ops.map(addIfMissing)
    : [addIfMissing(ops)];
}

export async function filterOps(ops: Op | Op[], spaceId: string): Promise<Op[]> {
  const clean = (op: Op): Op => {
    const { spaceId: _, ...rest } = op;
    return rest;
  };

  if (Array.isArray(ops)) {
    return ops.filter(op => op.spaceId === spaceId).map(clean);
  } else {
    return ops.spaceId === spaceId ? [clean(ops)] : [];
  }
}

export async function getSpaces(ops: Op[]): Promise<string[]> {
  const opsArray = Array.isArray(ops) ? ops : [ops];
  const spaceIds = opsArray
    .map(op => op.spaceId)
    .filter((id): id is string => typeof id === 'string');

  return Array.from(new Set(spaceIds));
}

//export async function addSources(currentOps: Op[], entityId: string, sourceEntityId: string, propertiesSourced: string[], source_url?: string, source_db_id?: string, toEntity?: string) {
export async function addSources({
  currentOps,
  entityId,
  sourceEntityId,
  propertiesSourced,
  source_url,
  source_db_id,
  toEntity,
  entityOnGeo,
  relations
}: {
  currentOps: Op[];
  entityId: string;
  sourceEntityId: string;
  propertiesSourced: string[];
  source_url?: string;
  source_db_id?: string;
  toEntity?: string;
  entityOnGeo: any;
  relations: any[];
}): Promise<{
    ops: Op[];
}> {
    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps: any;
    
    //Create relation from entity to source
    //Create relation entity that contains properties: source_url (web_url_property), Properties sourced

    const args = arguments[0];
    if (!("entityOnGeo" in args)) {
      console.log("SEARCHING ADD SOURCES")
      const entityOnGeo = await searchEntity({
          entityId: normalizeToUUID_STRING(entityId),
          spaceId: currSpaceId
      });
    }
    

    addOps = await processNewRelation({
        currenOps: [...ops, ...currentOps],
        spaceId: currSpaceId,
        entityOnGeo: entityOnGeo,
        fromEntityId: normalizeToUUID(entityId),
        toEntityId: normalizeToUUID(sourceEntityId),
        propertyId: normalizeToUUID(GEO_IDS.sourcesPropertyId),
    });
    ops.push(...addOps.ops);
    const relationEntityId = addOps.relationEntityId

    //console.log("SEARCHING relEntityOnGeo")
    //const relEntityOnGeo = await searchEntity({
    //    entityId: relationEntityId,
    //    spaceId: currSpaceId
    //});
    const relEntityOnGeo = relations.filter((entity: any) => entity?.id === normalizeToUUID_STRING(relationEntityId))?.[0]

    const values = []
    if (source_url) {
        if (!(await valuePropertyExistsOnGeo(currSpaceId, relEntityOnGeo, normalizeToUUID(GEO_IDS.webURLId)))) {
            values.push({property: normalizeToUUID(GEO_IDS.webURLId), value: source_url})
        }
    }

    if (source_db_id) {
        if (!(await valuePropertyExistsOnGeo(currSpaceId, relEntityOnGeo, normalizeToUUID(GEO_IDS.sourceDBIdentifier)))) {
            values.push({property: normalizeToUUID(GEO_IDS.sourceDBIdentifier), value: source_db_id})
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
        addOps = await processNewRelation({
            currenOps: [...ops, ...currentOps],
            spaceId: currSpaceId,
            entityOnGeo: relEntityOnGeo,
            fromEntityId: normalizeToUUID(relationEntityId),
            toEntityId: normalizeToUUID(property),
            propertyId: normalizeToUUID(GEO_IDS.propertiesSourced),
        });
        ops.push(...addOps.ops);


        if (toEntity) {
          //console.log("propSourcedRelEntityOnGeo")
          //let propSourcedRelEntityOnGeo = await searchEntity({
          //    entityId: addOps.relationEntityId,
          //    spaceId: currSpaceId
          //});

          let propSourcedRelEntityOnGeo = relations.filter((entity: any) => entity?.id === normalizeToUUID_STRING(addOps.relationEntityId))?.[0]
          addOps = await processNewRelation({
              currenOps: [...ops, ...currentOps],
              spaceId: currSpaceId,
              entityOnGeo: propSourcedRelEntityOnGeo,
              fromEntityId: normalizeToUUID(addOps.relationEntityId),
              toEntityId: normalizeToUUID(toEntity),
              propertyId: normalizeToUUID(GEO_IDS.relationsSourced),
          });
          ops.push(...addOps.ops);
        }
    }

    return { ops: (await addSpace(ops, currSpaceId)) };
}

export const getConcatenatedPlainText = (textArray?: any[]): string | undefined => {
    if (!Array.isArray(textArray) || textArray.length === 0) {
      return undefined;
    }
  
    return textArray
      .map(item => item?.plain_text ?? "")
      .join("")
      .trim() || undefined;
  };


import path from 'path';

export function readAllOpsFromFolder(): any[] {
  const folderPath = path.join(__dirname, '..', 'ethcc_testnet_ops'); // go up one level
  const allFiles = fs.readdirSync(folderPath);

  const opsFiles = allFiles.filter(file => /^ethcc_ops_\d+\.txt$/.test(file));

  let allOps: any[] = [];

  for (const file of opsFiles) {
    const filePath = path.join(folderPath, file);
    try {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      const ops = JSON.parse(fileContent);
      allOps.push(...ops);  // Assumes each file contains an array of ops
      console.log(`Read ${ops.length} ops from ${file}`);
    } catch (err) {
      console.error(`Failed to read or parse ${file}:`, err);
    }
  }

  return allOps;
}