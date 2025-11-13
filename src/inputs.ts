import { normalizeToUUID, propertyToIdMap } from './constants_v2.ts';
import { SystemIds, IdUtils} from "@graphprotocol/grc-20";

// --- Helpers ---

export function extractUrls(values: any[] = [], isApi: boolean = false): { url: string; propertyId: string }[] {
  const propKey = isApi ? "propertyId" : "property";

  return values
    .filter(v =>
      typeof v.value === "string" &&
      (/^https?:\/\//i.test(v.value) || /\.(com|org|net|io|co|fm)$/i.test(v.value))
    )
    .map(v => ({
      url: v.value,
      propertyId: v[propKey],
    }));
}

export function normalizeName(name: string = ""): string | null {
    if (name) {
        return name
            .toLowerCase()
            .replace(/\b(dr|mr|ms|mrs|the)\b/g, "") // drop common prefixes/articles
            .replace(/[^a-z0-9\s]/g, "")            // strip punctuation
            .replace(/\s+/g, " ")                   // collapse spaces
            .trim();
    } else {
        return null;
    }
}

export function levenshtein(a: string, b: string): number {
  const m = a.length, n = b.length;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;

  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (a[i - 1] === b[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1];
      } else {
        dp[i][j] = 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
      }
    }
  }
  return dp[m][n];
}

export function stringSimilarity(a: string, b: string): number {
  if (!a || !b) return 0;
  const dist = levenshtein(a, b);
  return 1 - dist / Math.max(a.length, b.length);
}


// --- Main matcher ---

export type LocalEntity = {
  internal_id: string;
  name?: string;
  values: { property: string; value: string }[];
  relations: any[];
  toEntity?: LocalEntity;
};

export type ApiEntity = {
  id: string;
  name: string | null;
  values: { propertyId: string; value?: string; string?: string }[];
  relations: any[];
};

export const isUrl = (str: string) => {
  if (!str || typeof str !== "string") return false;
  const domainPattern = /\.(com|org|net|io|gov|edu|co|fm|tv|me)(\/|$)/i;
  return str.startsWith("http://") || str.startsWith("https://") || domainPattern.test(str);
};


export function matchEntities(
  local: LocalEntity[],
  api: ApiEntity[],
): Record<string, string> {
  const matches: Record<string, string> = {};

  for (const localEntity of local) {
    // 1. Match on source db identifiers (via relations)
    let match;
    if (localEntity.relations) {
      for (const rel of localEntity.relations) {
        match = api.find(api =>
          api.relations?.some(r =>
            r.typeId == normalizeToUUID(propertyToIdMap["sources"]) &&
            r.toEntity.name == rel.toEntity.name &&
            Array.isArray(r?.entity?.values) &&
            r.entity.values.some(v =>
              v.propertyId == normalizeToUUID(propertyToIdMap["source_db_identifier"]) &&
              v.value === rel.entity.values.filter(v => v.property == normalizeToUUID(propertyToIdMap["source_db_identifier"])).value
            )
          )
        );
        if (match) break;
      }
    }
    if (match) {
      matches[localEntity.internal_id] = match.id;
      continue;
    }

    // 2️⃣ Match on URLs
    const matchedByUrl = api.find(apiEnt =>
      localEntity.values.some(localVal => isUrl(localVal.value)) &&
      apiEnt.values.some(apiVal => isUrl(apiVal.value || apiVal.string || ""))
    );

    if (matchedByUrl) {
      matches[localEntity.internal_id] = matchedByUrl.id;
      continue;
    }

    // 3️⃣ Fallback: match on combination of remaining values
    const matchedByValues = api.find(apiEnt =>
      localEntity.values.every(localVal =>
        apiEnt.values.some(
          apiVal =>
            (apiVal.value === localVal.value || apiVal.string === localVal.value) &&
            !isUrl(localVal.value)
        )
      )
    );

    if (matchedByValues) {
      matches[localEntity.internal_id] = matchedByValues.id;
    }
  }

  return matches;
}



// Cache to store already-built entities by table + id
export const entityCache: Record<string, Record<string, any>> = {};

export function buildEntityCached(
  row: any,
  breakdown: any,
  spaceId: string,
  tables: Record<string, any[]>,
  geoEntities: Record<string, any[]>,
  cache: Record<string, Record<string, any>>
): any {
  const tableName = breakdown.table;

  // --- cache check ---
  cache[tableName] = cache[tableName] || {};
  if (cache[tableName][row.id]) {
    return cache[tableName][row.id];
  }

  let geo_id: string | null = null;
  let entityOnGeo: any = null;

  const geoRows = geoEntities[tableName] ?? [];


  const existingSources: any[] = [];
  let match: any;
  let sourceMatch;

  // TODO - 
  // Filter to see whether there are sources relations being passed in [look at all sourced in the list]
  // - If so, look to see whether those source entities exist on Geo
  // - if so check for the source_db_identifier
  // If not, remove any entiites that have sources relations to those source entities already
  // Then check for a type + url property or type + name match in the remaining set of entities
  // IF entity exists on Geo then...
  // - CHECK AND SEE WHETHER RELATION ENTITIES EXIST ON GEO IF THIS ENTITY EXISTS ON GEO

  // --- build values ---
  const values = (breakdown.value_fields ?? []).flatMap((field: string) => {
    const val = row[field];
    return val != null
      ? [{
          spaceId,
          property: normalizeToUUID(propertyToIdMap[field]),
          value:
            typeof val === "object" && val instanceof Date
              ? val.toISOString()
              : String(val),
        }]
      : [];
  });

   // 🔹 STEP 1: Separate out source-type relations
  const sourceRelations = (breakdown.relations ?? []).filter(
    (rel: any) => rel.type === "sources"
  );
  const otherRelations = (breakdown.relations ?? []).filter(
    (rel: any) => rel.type !== "sources"
  );
  
  // --- build relations (handles both toEntity and entity sides) ---
  const relations = (sourceRelations ?? []).flatMap((rel: any) => {
    //const relatedItems = row[rel.type] ?? []; // now [{ to_id, entity_id }, ...]
    const relatedItems = Array.isArray(row[rel.type]) ? row[rel.type] : row[rel.type] ? [row[rel.type]] : []; // now [{ to_id, entity_id }, ...]
      return relatedItems.flatMap((relatedItem: any) => {
        if (rel.image) {
          console.log(`${rel.type} IMAGE FOUND`, relatedItem)
          return [
            {
              spaceId,
              type: normalizeToUUID(propertyToIdMap[rel.type]),
              toEntity: {
                internal_id: IdUtils.generate(),
                id: null,
                entityOnGeo: null,
                name: relatedItem,
                values: [],
                relations: [],
              },
              entity: null,
            },
          ];

        } else {

          const { to_id, entity_id } = relatedItem;

          // Build a scoped copy of geoEntities
          let scopedGeoEntities = geoEntities;

          // --- NEW: narrow geoEntities if needed ---
          if ((rel.type != "sources") && (rel.toEntityBreakdown?.not_unique && match)) {
            const allowedIds = new Set(
              (match.relations ?? [])
                .filter(r => r?.toEntityId)
                .map(r => String(r.toEntityId))
            );

            // shallow clone geoEntities, but replace only the relevant table with filtered subset
            scopedGeoEntities = {
              ...geoEntities,
              [rel.toEntityBreakdown.table]: (geoEntities[rel.toEntityBreakdown.table] ?? []).filter(
                g => allowedIds.has(String(g.id))
              ),
            };
          }
          if ((rel.type != "sources") && (rel.toEntityBreakdown?.not_unique && !match)) {
            scopedGeoEntities = {
              ...geoEntities,
              [rel.toEntityBreakdown.table]: [],
            };
          }

          // lookup the child entity using to_id
          const relatedRow = tables[rel.toEntityBreakdown.table].find(
          (r: any) => r.id == to_id
          );
          if (!relatedRow) return [];

          // build toEntity side
          const childEntity = buildEntityCached(
              relatedRow,
              rel.toEntityBreakdown,
              spaceId,
              tables,
              scopedGeoEntities,
              cache
          );

          // build entity side if entityBreakdown is provided
          let entitySide: any = null;
          if (rel.entityBreakdown) {
              const entityRow = tables[rel.entityBreakdown.table].find(
                  (r: any) => r.id == entity_id
              );
              if (entityRow) {
                  entitySide = buildEntityCached(
                  entityRow,
                  rel.entityBreakdown,
                  spaceId,
                  tables,
                  geoEntities,
                  cache
                  );
              }
          }

          
          if (rel.type == "sources" && childEntity.entityOnGeo) { //Todo - Check that this doesnt pull anything in if the child entity is empty (even if it just has a type...)
              console.log("SOURCE FOUND")
              const hasSourceDbIdentifier = childEntity?.entityOnGeo?.values?.some(
                v => v.propertyId === String(normalizeToUUID(propertyToIdMap["source_db_identifier"]))
              );
              if (hasSourceDbIdentifier) {
                existingSources.push(childEntity.entityOnGeo.id)
              }
              
              //console.log(childEntity.entityOnGeo)
              if (!match) {                  
                  const sourceTypeId = String(normalizeToUUID(propertyToIdMap["sources"]));
                  const sourceDbPropId = String(normalizeToUUID(propertyToIdMap["source_db_identifier"]));
                  const sourceDbValue = String(
                    entitySide?.values?.find(v => String(v.property) == sourceDbPropId)?.value || ""
                  );
                  match = geoRows.find(p =>
                      p.relations?.some(r =>
                          String(r.typeId) == sourceTypeId &&
                          String(r.toEntityId) == String(childEntity.entityOnGeo.id) &&
                          Array.isArray(r?.entity?.values) &&
                          r.entity.values.some(v =>
                              String(v.propertyId) == sourceDbPropId &&
                              String(v.value) == sourceDbValue
                          )
                      )
                  );
              }
              
          }
          

      return [
        {
          spaceId,
          type: normalizeToUUID(propertyToIdMap[rel.type]),
          toEntity: childEntity,
          entity: entitySide,
        },
      ];
      }
    });
});

  // --- type relations ---
  for (const type of breakdown.types) {
    relations.push({
      spaceId,
      type: SystemIds.TYPES_PROPERTY,
      toEntity: {
        internal_id: IdUtils.generate(),
        id: type,
        entityOnGeo: null,
        name: null,
        values: [],
        relations: [],
      },
      entity: null,
    });
  }

  //TODO - Instead of exact name match, check for url properties first...
  // Can get this from the values array matching against the other values array in the geoAPI response. This is the area that I can do a confidence score matching like chatGPT recommended

// 2. Match on URL + property
if (!match) {
  const localUrls = extractUrls(values, false);

  match = geoRows.find(p => {
    const apiUrls = extractUrls(p.values, true);

    return (
      // must have correct type
      p.relations?.some(r =>
        String(r.typeId) == String(SystemIds.TYPES_PROPERTY) &&
        String(r.toEntityId) == String(breakdown.types[0])
      ) &&

      // must not have a source already in existingSources
      p.relations?.every(r =>
        !(String(r.typeId) == String(normalizeToUUID(propertyToIdMap["sources"])) &&
          existingSources.includes(String(r.toEntityId)))
      ) &&

      // must share a URL AND property
      localUrls.some(local =>
        apiUrls.some(api => String(api.url) == String(local.url) && String(api.propertyId) == String(local.propertyId))
      )
    );
  });

  console.log(localUrls);
}

// 3. Match on name similarity
if (!match && row.name) {
    const localName = normalizeName(row.name);
    let bestScore = 0;
    let bestMatch: any = null;

    for (const p of geoRows) {
        // enforce type + exclude existingSources
        const valid =
            p.relations?.some(r =>
                String(r.typeId) == String(SystemIds.TYPES_PROPERTY) &&
                String(r.toEntityId) == String(breakdown.types[0])
            ) &&
            p.relations?.every(r =>
                !(String(r.typeId) == String(normalizeToUUID(propertyToIdMap["sources"])) &&
                existingSources.includes(String(r.toEntityId)))
            );

        if (!valid) continue;

        // ✅ check URL/property alignment
        let mismatch = false;
        for (const localVal of values) {
            if (typeof localVal.value != "string") continue;
            const localIsUrl = (/^https?:\/\//i.test(String(localVal.value)) || /\.(com|org|net|io|co|fm)$/i.test(String(localVal.value)));
            if (!localIsUrl) continue;

            // look for same propertyId in API values
            const apiVal = p.values?.find(v => String(v.propertyId) === String(localVal.property));
            if (apiVal && typeof apiVal.value == "string") {
                const apiIsUrl = (/^https?:\/\//i.test(String(apiVal.value)) || /\.(com|org|net|io|co|fm)$/i.test(String(apiVal.value)));
                if (apiIsUrl && String(apiVal.value) != String(localVal.value)) {
                    mismatch = true; // same property, but URL differs
                    break;
                }
            }
        }
        if (mismatch) continue; // 🚫 reject this candidate

        const apiName = normalizeName(p.name);
        const score = stringSimilarity(localName, apiName);

        if (score > bestScore) {
            bestScore = score;
            bestMatch = p;
        }
    }

    console.log(bestScore);
    if (bestScore > 0.9) match = bestMatch; // adjust threshold as needed
}

  if (match) {
    geo_id = match.id;
    entityOnGeo = match;
  }
  
  const other_relations = (otherRelations ?? []).flatMap((rel: any) => {
      //const relatedItems = row[rel.type] ?? []; // now [{ to_id, entity_id }, ...]
      const relatedItems = Array.isArray(row[rel.type]) ? row[rel.type] : row[rel.type] ? [row[rel.type]] : []; // now [{ to_id, entity_id }, ...]
        return relatedItems.flatMap((relatedItem: any) => {
          if (rel.image) {
            console.log(`${rel.type} IMAGE FOUND`, relatedItem)
            return [
              {
                spaceId,
                type: normalizeToUUID(propertyToIdMap[rel.type]),
                toEntity: {
                  internal_id: IdUtils.generate(),
                  id: null,
                  entityOnGeo: null,
                  name: relatedItem,
                  values: [],
                  relations: [],
                },
                entity: null,
              },
            ];

          } else {

            const { to_id, entity_id } = relatedItem;

            // Build a scoped copy of geoEntities
            let scopedGeoEntities = geoEntities;

            // --- NEW: narrow geoEntities if needed ---
            if ((rel.type != "sources") && (rel.toEntityBreakdown?.not_unique && match)) {
              const allowedIds = new Set(
                (match.relations ?? [])
                  .filter(r => r?.toEntityId)
                  .map(r => String(r.toEntityId))
              );

              // shallow clone geoEntities, but replace only the relevant table with filtered subset
              scopedGeoEntities = {
                ...geoEntities,
                [rel.toEntityBreakdown.table]: (geoEntities[rel.toEntityBreakdown.table] ?? []).filter(
                  g => allowedIds.has(String(g.id))
                ),
              };
            }
            if ((rel.type != "sources") && (rel.toEntityBreakdown?.not_unique && !match)) {
              scopedGeoEntities = {
                ...geoEntities,
                [rel.toEntityBreakdown.table]: [],
              };
            }

            // lookup the child entity using to_id
            const relatedRow = tables[rel.toEntityBreakdown.table].find(
            (r: any) => r.id == to_id
            );
            if (!relatedRow) return [];

            // build toEntity side
            const childEntity = buildEntityCached(
                relatedRow,
                rel.toEntityBreakdown,
                spaceId,
                tables,
                scopedGeoEntities,
                cache
            );

            // build entity side if entityBreakdown is provided
            let entitySide: any = null;
            if (rel.entityBreakdown) {
                const entityRow = tables[rel.entityBreakdown.table].find(
                    (r: any) => r.id == entity_id
                );
                if (entityRow) {
                    entitySide = buildEntityCached(
                    entityRow,
                    rel.entityBreakdown,
                    spaceId,
                    tables,
                    geoEntities,
                    cache
                    );
                }
            }

            
            if (rel.type == "sources" && childEntity.entityOnGeo) { //Todo - Check that this doesnt pull anything in if the child entity is empty (even if it just has a type...)
                console.log("SOURCE FOUND")
                const hasSourceDbIdentifier = childEntity?.entityOnGeo?.values?.some(
                  v => v.propertyId === String(normalizeToUUID(propertyToIdMap["source_db_identifier"]))
                );
                if (hasSourceDbIdentifier) {
                  existingSources.push(childEntity.entityOnGeo.id)
                }
                
                //console.log(childEntity.entityOnGeo)
                if (!match) {                  
                    const sourceTypeId = String(normalizeToUUID(propertyToIdMap["sources"]));
                    const sourceDbPropId = String(normalizeToUUID(propertyToIdMap["source_db_identifier"]));
                    const sourceDbValue = String(
                      entitySide?.values?.find(v => String(v.property) == sourceDbPropId)?.value || ""
                    );
                    match = geoRows.find(p =>
                        p.relations?.some(r =>
                            String(r.typeId) == sourceTypeId &&
                            String(r.toEntityId) == String(childEntity.entityOnGeo.id) &&
                            Array.isArray(r?.entity?.values) &&
                            r.entity.values.some(v =>
                                String(v.propertyId) == sourceDbPropId &&
                                String(v.value) == sourceDbValue
                            )
                        )
                    );
                }
                
            }
            

        return [
          {
            spaceId,
            type: normalizeToUUID(propertyToIdMap[rel.type]),
            toEntity: childEntity,
            entity: entitySide,
          },
        ];
        }
      });
  });
  relations.push(...other_relations)


  // --- final entity ---
  const entity = {
    internal_id: IdUtils.generate(),
    id: geo_id,
    entityOnGeo: entityOnGeo,
    name: row.name,
    values,
    relations,
  };

  // --- cache save ---
  cache[tableName][row.id] = entity;

  return entity;
}


export function buildEntityCached_orig(
  row: any,
  breakdown: any,
  spaceId: string,
  tables: Record<string, any[]>,
  geoEntities: Record<string, any[]>,
  cache: Record<string, Record<string, any>>
): any {
  const tableName = breakdown.table;

  // --- cache check ---
  cache[tableName] = cache[tableName] || {};
  if (cache[tableName][row.id]) {
    return cache[tableName][row.id];
  }

  let geo_id: string | null = null;
  let entityOnGeo: any = null;

  const geoRows = geoEntities[tableName] ?? [];


  const existingSources: any[] = [];
  let match: any;
  let sourceMatch;

  // TODO - 
  // Filter to see whether there are sources relations being passed in [look at all sourced in the list]
  // - If so, look to see whether those source entities exist on Geo
  // - if so check for the source_db_identifier
  // If not, remove any entiites that have sources relations to those source entities already
  // Then check for a type + url property or type + name match in the remaining set of entities
  // IF entity exists on Geo then...
  // - CHECK AND SEE WHETHER RELATION ENTITIES EXIST ON GEO IF THIS ENTITY EXISTS ON GEO

  // --- build values ---
  const values = (breakdown.value_fields ?? []).flatMap((field: string) => {
    const val = row[field];
    return val != null
      ? [{
          spaceId,
          property: normalizeToUUID(propertyToIdMap[field]),
          value:
            typeof val === "object" && val instanceof Date
              ? val.toISOString()
              : String(val),
        }]
      : [];
  });

  // --- build relations (handles both toEntity and entity sides) ---
  const relations = (breakdown.relations ?? []).flatMap((rel: any) => {
    //const relatedItems = row[rel.type] ?? []; // now [{ to_id, entity_id }, ...]
    const relatedItems = Array.isArray(row[rel.type]) ? row[rel.type] : row[rel.type] ? [row[rel.type]] : []; // now [{ to_id, entity_id }, ...]
      return relatedItems.flatMap((relatedItem: any) => {
        if (rel.image) {
          console.log(`${rel.type} IMAGE FOUND`, relatedItem)
          return [
            {
              spaceId,
              type: normalizeToUUID(propertyToIdMap[rel.type]),
              toEntity: {
                internal_id: IdUtils.generate(),
                id: null,
                entityOnGeo: null,
                name: relatedItem,
                values: [],
                relations: [],
              },
              entity: null,
            },
          ];

        } else {

          const { to_id, entity_id } = relatedItem;

          // lookup the child entity using to_id
          const relatedRow = tables[rel.toEntityBreakdown.table].find(
          (r: any) => r.id == to_id
          );
          if (!relatedRow) return [];

          // build toEntity side
          const childEntity = buildEntityCached(
              relatedRow,
              rel.toEntityBreakdown,
              spaceId,
              tables,
              geoEntities,
              cache
          );

          // build entity side if entityBreakdown is provided
          let entitySide: any = null;
          if (rel.entityBreakdown) {
              const entityRow = tables[rel.entityBreakdown.table].find(
                  (r: any) => r.id == entity_id
              );
              if (entityRow) {
                  entitySide = buildEntityCached(
                  entityRow,
                  rel.entityBreakdown,
                  spaceId,
                  tables,
                  geoEntities,
                  cache
                  );
              }
          }

          
          if (rel.type == "sources" && childEntity.entityOnGeo) { //Todo - Check that this doesnt pull anything in if the child entity is empty (even if it just has a type...)
              console.log("SOURCE FOUND")
              const hasSourceDbIdentifier = childEntity?.entityOnGeo?.values?.some(
                v => v.propertyId === String(normalizeToUUID(propertyToIdMap["source_db_identifier"]))
              );
              if (hasSourceDbIdentifier) {
                existingSources.push(childEntity.entityOnGeo.id)
              }
              
              //console.log(childEntity.entityOnGeo)
              if (!match) {                  
                  const sourceTypeId = String(normalizeToUUID(propertyToIdMap["sources"]));
                  const sourceDbPropId = String(normalizeToUUID(propertyToIdMap["source_db_identifier"]));
                  const sourceDbValue = String(
                    entitySide?.values?.find(v => String(v.property) == sourceDbPropId)?.value || ""
                  );
                  match = geoRows.find(p =>
                      p.relations?.some(r =>
                          String(r.typeId) == sourceTypeId &&
                          String(r.toEntityId) == String(childEntity.entityOnGeo.id) &&
                          Array.isArray(r?.entity?.values) &&
                          r.entity.values.some(v =>
                              String(v.propertyId) == sourceDbPropId &&
                              String(v.value) == sourceDbValue
                          )
                      )
                  );
              }
              
          }
          

      return [
        {
          spaceId,
          type: normalizeToUUID(propertyToIdMap[rel.type]),
          toEntity: childEntity,
          entity: entitySide,
        },
      ];
      }
    });
});

  // --- type relations ---
  for (const type of breakdown.types) {
    relations.push({
      spaceId,
      type: SystemIds.TYPES_PROPERTY,
      toEntity: {
        internal_id: IdUtils.generate(),
        id: type,
        entityOnGeo: null,
        name: null,
        values: [],
        relations: [],
      },
      entity: null,
    });
  }


  //TODO - Instead of exact name match, check for url properties first...
  // Can get this from the values array matching against the other values array in the geoAPI response. This is the area that I can do a confidence score matching like chatGPT recommended

// 2. Match on URL + property
if (!match) {
  const localUrls = extractUrls(values, false);

  match = geoRows.find(p => {
    const apiUrls = extractUrls(p.values, true);

    return (
      // must have correct type
      p.relations?.some(r =>
        String(r.typeId) == String(SystemIds.TYPES_PROPERTY) &&
        String(r.toEntityId) == String(breakdown.types[0])
      ) &&

      // must not have a source already in existingSources
      p.relations?.every(r =>
        !(String(r.typeId) == String(normalizeToUUID(propertyToIdMap["sources"])) &&
          existingSources.includes(String(r.toEntityId)))
      ) &&

      // must share a URL AND property
      localUrls.some(local =>
        apiUrls.some(api => String(api.url) == String(local.url) && String(api.propertyId) == String(local.propertyId))
      )
    );
  });

  console.log(localUrls);
}

// 3. Match on name similarity
if (!match && row.name) {
    const localName = normalizeName(row.name);
    let bestScore = 0;
    let bestMatch: any = null;

    for (const p of geoRows) {
        // enforce type + exclude existingSources
        const valid =
            p.relations?.some(r =>
                String(r.typeId) == String(SystemIds.TYPES_PROPERTY) &&
                String(r.toEntityId) == String(breakdown.types[0])
            ) &&
            p.relations?.every(r =>
                !(String(r.typeId) == String(normalizeToUUID(propertyToIdMap["sources"])) &&
                existingSources.includes(String(r.toEntityId)))
            );

        if (!valid) continue;

        // ✅ check URL/property alignment
        let mismatch = false;
        for (const localVal of values) {
            if (typeof localVal.value != "string") continue;
            const localIsUrl = (/^https?:\/\//i.test(String(localVal.value)) || /\.(com|org|net|io|co|fm)$/i.test(String(localVal.value)));
            if (!localIsUrl) continue;

            // look for same propertyId in API values
            const apiVal = p.values?.find(v => String(v.propertyId) === String(localVal.property));
            if (apiVal && typeof apiVal.value == "string") {
                const apiIsUrl = (/^https?:\/\//i.test(String(apiVal.value)) || /\.(com|org|net|io|co|fm)$/i.test(String(apiVal.value)));
                if (apiIsUrl && String(apiVal.value) != String(localVal.value)) {
                    mismatch = true; // same property, but URL differs
                    break;
                }
            }
        }
        if (mismatch) continue; // 🚫 reject this candidate

        const apiName = normalizeName(p.name);
        const score = stringSimilarity(localName, apiName);

        if (score > bestScore) {
            bestScore = score;
            bestMatch = p;
        }
    }

    console.log(bestScore);
    if (bestScore > 0.9) match = bestMatch; // adjust threshold as needed
}

  if (match) {
    geo_id = match.id;
    entityOnGeo = match;
  }
  

  // --- final entity ---
  const entity = {
    internal_id: IdUtils.generate(),
    id: geo_id,
    entityOnGeo: entityOnGeo,
    name: row.name,
    values,
    relations,
  };

  // --- cache save ---
  cache[tableName][row.id] = entity;

  return entity;
}

export function buildEntityCached_v2(
  row: any,
  breakdown: any,
  spaceId: string,
  tables: Record<string, any[]>,
  geoEntities: Record<string, any[]>,
  cache: Record<string, Record<string, any>>
): any {
  const tableName = breakdown.table;

  // --- cache check ---
  cache[tableName] = cache[tableName] || {};
  if (cache[tableName][row.id]) {
    return cache[tableName][row.id];
  }

  let geo_id: string | null = null;
  let entityOnGeo: any = null;
  const geoRows = geoEntities[tableName] ?? [];

  const existingSources: string[] = [];
  let match: any = null;

  // --- build values ---
  const values = (breakdown.value_fields ?? []).flatMap((field: string) => {
    const val = row[field];
    return val != null
      ? [
          {
            spaceId,
            property: normalizeToUUID(propertyToIdMap[field]),
            value:
              typeof val === "object" && val instanceof Date
                ? val.toISOString()
                : String(val),
          },
        ]
      : [];
  });

  // 🔹 STEP 1: Separate out source-type relations
  const sourceRelations = (breakdown.relations ?? []).filter(
    (rel: any) => rel.type === "sources"
  );
  const otherRelations = (breakdown.relations ?? []).filter(
    (rel: any) => rel.type !== "sources"
  );

  // 🔹 STEP 2: Handle SOURCE relations and try to identify existing Geo matches
  for (const rel of sourceRelations) {
    const relatedItems = Array.isArray(row[rel.type])
      ? row[rel.type]
      : row[rel.type]
      ? [row[rel.type]]
      : [];

    for (const relatedItem of relatedItems) {
      const { to_id, entity_id } = relatedItem;

      if (!rel?.toEntityBreakdown?.table) {
        console.log(rel)
      }
      const relatedRow = tables[rel?.toEntityBreakdown?.table].find(
        (r: any) => r.id == to_id
      );
      if (!relatedRow) continue;

      const childEntity = buildEntityCached(
        relatedRow,
        rel.toEntityBreakdown,
        spaceId,
        tables,
        geoEntities,
        cache
      );

      let entitySide: any = null;
      if (rel.entityBreakdown) {
        const entityRow = tables[rel.entityBreakdown.table].find(
          (r: any) => r.id == entity_id
        );
        if (entityRow) {
          entitySide = buildEntityCached(
            entityRow,
            rel.entityBreakdown,
            spaceId,
            tables,
            geoEntities,
            cache
          );
        }
      }

      // --- Check if source entity exists on Geo
      if (childEntity.entityOnGeo) {
        const hasSourceDbIdentifier = childEntity.entityOnGeo?.values?.some(
          (v) =>
            v.propertyId ===
            String(normalizeToUUID(propertyToIdMap["source_db_identifier"]))
        );

        if (hasSourceDbIdentifier) {
          existingSources.push(childEntity.entityOnGeo.id);
        }

        // Attempt to match entity by sources
        if (!match) {
          const sourceTypeId = String(
            normalizeToUUID(propertyToIdMap["sources"])
          );
          const sourceDbPropId = String(
            normalizeToUUID(propertyToIdMap["source_db_identifier"])
          );
          const sourceDbValue = String(
            entitySide?.values?.find(
              (v) => String(v.property) == sourceDbPropId
            )?.value || ""
          );

          match = geoRows.find((p) =>
            p.relations?.some(
              (r) =>
                String(r.typeId) == sourceTypeId &&
                String(r.toEntityId) ==
                  String(childEntity.entityOnGeo.id) &&
                Array.isArray(r?.entity?.values) &&
                r.entity.values.some(
                  (v) =>
                    String(v.propertyId) == sourceDbPropId &&
                    String(v.value) == sourceDbValue
                )
            )
          );
        }
      }
    }
  }

  // 🔹 STEP 3: Matching Logic (URL + Name similarity)
  if (!match) {
    // --- Match by URL + property ---
    const localUrls = extractUrls(values, false);
    match = geoRows.find((p) => {
      const apiUrls = extractUrls(p.values, true);
      return (
        // type must match
        p.relations?.some(
          (r) =>
            String(r.typeId) == String(SystemIds.TYPES_PROPERTY) &&
            String(r.toEntityId) == String(breakdown.types[0])
        ) &&
        // must not already be linked to an existing source
        p.relations?.every(
          (r) =>
            !(
              String(r.typeId) ==
                String(normalizeToUUID(propertyToIdMap["sources"])) &&
              existingSources.includes(String(r.toEntityId))
            )
        ) &&
        // must share URL and property
        localUrls.some((local) =>
          apiUrls.some(
            (api) =>
              String(api.url) == String(local.url) &&
              String(api.propertyId) == String(local.propertyId)
          )
        )
      );
    });
  }

  if (!match && row.name) {
    // --- Match by Name Similarity ---
    const localName = normalizeName(row.name);
    let bestScore = 0;
    let bestMatch: any = null;

    for (const p of geoRows) {
      const valid =
        p.relations?.some(
          (r) =>
            String(r.typeId) == String(SystemIds.TYPES_PROPERTY) &&
            String(r.toEntityId) == String(breakdown.types[0])
        ) &&
        p.relations?.every(
          (r) =>
            !(
              String(r.typeId) ==
                String(normalizeToUUID(propertyToIdMap["sources"])) &&
              existingSources.includes(String(r.toEntityId))
            )
        );

      if (!valid) continue;

      // skip mismatching URLs with same propertyId
      let mismatch = false;
      for (const localVal of values) {
        if (typeof localVal.value !== "string") continue;
        const localIsUrl = /^https?:\/\//i.test(localVal.value);
        if (!localIsUrl) continue;

        const apiVal = p.values?.find(
          (v) => String(v.propertyId) === String(localVal.property)
        );
        if (apiVal && typeof apiVal.value === "string") {
          const apiIsUrl = /^https?:\/\//i.test(apiVal.value);
          if (apiIsUrl && apiVal.value !== localVal.value) {
            mismatch = true;
            break;
          }
        }
      }
      if (mismatch) continue;

      const apiName = normalizeName(p.name);
      const score = stringSimilarity(localName, apiName);

      if (score > bestScore) {
        bestScore = score;
        bestMatch = p;
      }
    }

    if (bestScore > 0.9) match = bestMatch;
  }

  if (match) {
    geo_id = match.id;
    entityOnGeo = match;
  }

  // 🔹 STEP 4: Build remaining relations
  const relations = otherRelations.flatMap((rel: any) => {
    const relatedItems = Array.isArray(row[rel.type])
      ? row[rel.type]
      : row[rel.type]
      ? [row[rel.type]]
      : [];

    return relatedItems.flatMap((relatedItem: any) => {
      if (rel.image) {
        console.log(`${rel.type} IMAGE FOUND`, relatedItem)
        return [
          {
            spaceId,
            type: normalizeToUUID(propertyToIdMap[rel.type]),
            toEntity: {
              internal_id: IdUtils.generate(),
              id: null,
              entityOnGeo: null,
              name: relatedItem,
              values: [],
              relations: [],
            },
            entity: null,
          },
        ];

      } else {
      // --- inside the relations.flatMap block ---

        const { to_id, entity_id } = relatedItem;

      
        if (!rel?.toEntityBreakdown?.table) {
          console.log(rel)
        }
        // lookup the child entity using to_id
        const relatedRow = tables[rel?.toEntityBreakdown?.table].find(
          (r: any) => r.id == to_id
        );
        if (!relatedRow) return [];

        // Build a scoped copy of geoEntities
        let scopedGeoEntities = geoEntities;

        // --- NEW: narrow geoEntities if needed ---
        if (rel.toEntityBreakdown?.not_unique && match) {
          const allowedIds = new Set(
            (match.relations ?? [])
              .filter(r => r?.toEntityId)
              .map(r => String(r.toEntityId))
          );

          // shallow clone geoEntities, but replace only the relevant table with filtered subset
          scopedGeoEntities = {
            ...geoEntities,
            [rel.toEntityBreakdown.table]: (geoEntities[rel.toEntityBreakdown.table] ?? []).filter(
              g => allowedIds.has(String(g.id))
            ),
          };
        }

        // build toEntity side using scoped geoEntities
        const childEntity = buildEntityCached(
          relatedRow,
          rel.toEntityBreakdown,
          spaceId,
          tables,
          scopedGeoEntities,
          cache
        );

        // build entity side if entityBreakdown is provided
        let entitySide: any = null;
        if (rel.entityBreakdown) {
          const entityRow = tables[rel.entityBreakdown.table].find(
            (r: any) => r.id == entity_id
          );
          if (entityRow) {
            entitySide = buildEntityCached(
              entityRow,
              rel.entityBreakdown,
              spaceId,
              tables,
              geoEntities,
              cache
            );
          }
        }


        return [
          {
            spaceId,
            type: normalizeToUUID(propertyToIdMap[rel.type]),
            toEntity: childEntity,
            entity: entitySide,
          },
        ];
      }
    });
  });

  // --- type relations ---
  for (const type of breakdown.types) {
    relations.push({
      spaceId,
      type: SystemIds.TYPES_PROPERTY,
      toEntity: {
        internal_id: IdUtils.generate(),
        id: type,
        entityOnGeo: null,
        name: null,
        values: [],
        relations: [],
      },
      entity: null,
    });
  }

  // 🔹 STEP 5: Build and cache final entity
  const entity = {
    internal_id: IdUtils.generate(),
    id: geo_id,
    entityOnGeo,
    name: row.name,
    values,
    relations,
  };

  cache[tableName][row.id] = entity;
  return entity;
}




export function normalizeValue(v: any): string {
  if (v.value) return String(v.value);     // input style
  if (v.string) return String(v.string);   // Geo API style
  if (v.number) return String(v.number);
  if (v.boolean) return String(v.boolean);
  if (v.time) return String(v.time);
  if (v.point) return String(v.point); //JSON.stringify(v.point); // if needed
  //if (v.unit !== undefined) return String(v.unit);
  //if (v.language !== undefined) return String(v.language);
  return "";
}

export function flattenEntity(entity: any): any {
  if (!entity) return null;

  return {
    ...entity,
    // flatten values
    values: (entity.values?.nodes ?? []).map((v: any) => ({
      spaceId: v.spaceId,
      propertyId: v.propertyId,
      value: normalizeValue(v),
    })),
    // flatten relations recursively
    relations: (entity.relations?.nodes ?? []).map((r: any) => ({
      ...r,
      entity: r.entity ? flattenEntity(r.entity) : null,
    })),
  };
}

export function flatten_api_response(response: any[]): any[] {
  return response.map(item => ({
    ...item,
    values: (item.values?.nodes ?? []).map((v: any) => ({
      spaceId: v.spaceId,
      propertyId: v.propertyId,
      value: normalizeValue(v),
    })),
    relations: (item.relations?.nodes ?? []).map((r: any) => ({
      ...r,
      entity: r.entity ? flattenEntity(r.entity) : null,
    })),
  }));
}


export type Value = {
  spaceId: string;
  property: string;
  value: string;
};

export type Relation = {
  spaceId: string;
  type: string;
  toEntity: Entity;
  entity: Entity;
};

export type Entity = {
    internal_id: string;
    id: string;
    entityOnGeo: any
    name: string;
    values: Value[]
    relations: Relation[]
};


// --- Search Helpers ---

const mainnet_query_url = "https://hypergraph.up.railway.app/graphql";
//const testnet_query_url = "https://geo-conduit.up.railway.app/graphql";
//const testnet_query_url = "https://hypergraph-v2-testnet.up.railway.app/graphql"
const testnet_query_url = "https://api-testnet.geobrowser.io/graphql"
const QUERY_URL = testnet_query_url;

export async function fetchWithRetry(query: string, variables: any, retries = 3, delay = 200) {
    //console.log("FETCHING...")
    for (let i = 0; i < retries; i++) {
        const response = await fetch(QUERY_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ query, variables }),
        });

        if (response.ok) {
           //console.log("DONE FETCHING")
            return await response.json();
        }

        if (i < retries - 1) {
            // Optional: only retry on certain error statuses
            console.log("Retry #", i)
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



export async function searchEntities({
  name, // Note: For V1, can assume always have name and type, but it is possible that there will not be a name to associate this with? 
  type,
  spaceId,
  property,
  searchText,
  typeId,
  notTypeId
}: {
  name?: string;
  type: string[];
  spaceId?: string[];
  property?: string;
  searchText?: string | string[];
  typeId?: string;
  notTypeId?: string;
}) {
  
  await new Promise(resolve => setTimeout(resolve, 200));

  const query = `
    query GetEntities(
      ${name ?  '$name: String!': ''}
      ${spaceId ? '$spaceId: [UUID!]' : ''}
      $type: [UUID!]
    ) {
      entities(
        filter: {
          ${name ? 'name: {isInsensitive: $name},' : ''}  
          ${spaceId ? 'spaceIds: {containedBy: $spaceId},' : ''}  
          relations: {some: {typeId: {is: "8f151ba4-de20-4e3c-9cb4-99ddf96f48f1"}, toEntityId: {in: $type}}},
        }
      ) {
        id
        name
        values {
            nodes {
                spaceId
                propertyId
                string
                language
                time
                number
                unit
                boolean
                point
            }
        }
        relations {
            nodes {
                id
                spaceId
                fromEntityId
                toEntityId
                typeId
                verified
                position
                toSpaceId
                entityId
                entity {
                  id
                  name
                  values {
                      nodes {
                          spaceId
                          propertyId
                          string
                          language
                          time
                          number
                          unit
                          boolean
                          point
                      }
                  }
                  relations {
                      nodes {
                          id
                          spaceId
                          fromEntityId
                          toEntityId
                          typeId
                          verified
                          position
                          toSpaceId
                          entityId
                      }
                  }
                }
            }
        }
      }
    }
  `;


  const variables: Record<string, any> = {
    name: name,
    type: type,
    spaceId: spaceId
  };


  const data = await fetchWithRetry(query, variables);
  const entities = data?.data?.entities;
  return entities

  if (entities?.length === 1) {
    return entities[0]?.id;
  } else if (entities?.length > 1) {
    console.error("DUPLICATE ENTITIES FOUND...");
    console.log(entities);
    return entities[0]?.id;
  }

  return null;
}

