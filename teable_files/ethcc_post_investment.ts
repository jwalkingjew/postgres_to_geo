import { PROD_TABLE_IDS } from "./src/teable-meta";
import { searchEntities, searchEntity, hasBeenEdited, searchOps, normalizeToUUID, propertyToIdMap, relationPropertyExistsOnGeo, valuePropertyExistsOnGeo, processNewRelation, GEO_IDS,  addSpace, addSources } from "./src/constants_v2";

import { type DataType,  type Op } from "@graphprotocol/grc-20";
import { Graph, Position, Id, Ipfs, SystemIds } from "@graphprotocol/grc-20";
import { processTag } from "./ethcc_post_tag";
import { processProject } from "./ethcc_post_project";

//async function iterateInvestors(currentOps: Array<Op>, teableClient: any, table_id: string, property_value: string, search_property: string, projectEntityId: string, fundraisingRoundEntityId: string): Promise<Op[]> {
async function iterateInvestors({
  currentOps,
  teableClient,
  table_id,
  property_value,
  search_property,
  projectEntityId,
  fundraisingRoundEntityId
}: {
  currentOps: Array<Op>;
  teableClient: any;
  table_id: string;
  property_value: string;
  search_property: string;
  projectEntityId: string;
  fundraisingRoundEntityId: string;
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

    filter_obj = JSON.parse(`{"conjunction":"and","filterSet":[{"fieldId":"${search_property}","operator":"is","value":"${property_value}"}]}`);

    while (true) {
        const page = await teableClient.get_records(table_id, filter_obj, take, skip);
        
        if (!page || page.length === 0) break;

        for (const record of page) {
            if (record?.fields?.['investor']) {
                addOps = await addInvestor({
                    currentOps: [...ops, ...currentOps],
                    teableClient: teableClient,
                    project_geoId: projectEntityId,
                    page: record,
                    fundingRoundEntity: fundraisingRoundEntityId,
                });
                ops.push(...addOps.ops);
            }
        }

        skip += take;
    }

    return { ops };
}

//async function addInvestor(currentOps: Array<Op>, teableClient: any, project_geoId: string, page: any, fundingRoundEntity: string): Promise<Op | Op[]> {
async function addInvestor({
  currentOps,
  teableClient,
  project_geoId,
  page,
  fundingRoundEntity,
}: {
  currentOps: Array<Op>;
  teableClient: any;
  project_geoId: string;
  page: any;
  fundingRoundEntity: string;
}): Promise<{
    ops: Op[];
}> {
    //add a relation from investor to project if it doesnt already exist.
    //add investors relation back from the project, using the same relation entity
    //Whether it already exists or not, add a relatoin from that investor relation entity to the funding round entity

    const propertiesSourced = [];
    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let investor_geoId: string;
    let entityOnGeo;

    addOps = await processProject({
        currentOps: [...ops, ...currentOps],
        projectId: page?.fields?.['investor']?.id,
        teableClient: teableClient
    });
    ops.push(...addOps.ops);
    investor_geoId = addOps.id;

    // Add investor to funding round entity
    entityOnGeo = await searchEntity({
        entityId: fundingRoundEntity,
        spaceId: currSpaceId
    });
    addOps = await processNewRelation({
        currenOps: [...ops, ...currentOps],
        spaceId: currSpaceId,
        entityOnGeo: entityOnGeo,
        fromEntityId: fundingRoundEntity,
        toEntityId: investor_geoId,
        propertyId: propertyToIdMap['investor'],
    });
    ops.push(...addOps.ops);

    // Add investor to properties sourced if it is added here...
    
    propertiesSourced.push('investor')
    if (propertiesSourced.length > 0) {
        let source_id = GEO_IDS.coincarp
        let source_url = page?.fields?.['source'].toString()

        if (page?.fields?.['source'].toString().includes("coincarp.com")) {
            
            addOps = await addSources({
                currentOps: [...ops, ...currentOps],
                entityId: fundingRoundEntity,
                sourceEntityId: source_id,
                propertiesSourced: propertiesSourced.map(property => propertyToIdMap[property]),
                source_url: source_url,
                source_db_id: undefined,
                toEntity: investor_geoId
            })
            ops.push(...addOps.ops);
        }
        propertiesSourced.length = 0
    }
    
    // Add investment to investor entity
    entityOnGeo = await searchEntity({
        entityId: investor_geoId,
        spaceId: currSpaceId
    });
    addOps = await processNewRelation({
        currenOps: [...ops, ...currentOps],
        spaceId: currSpaceId,
        entityOnGeo: entityOnGeo,
        fromEntityId: investor_geoId,
        toEntityId: project_geoId,
        propertyId: propertyToIdMap['invested_in'],
    });
    ops.push(...addOps.ops);
    let relationEntity = addOps.relationEntityId;
    
    // Add investor type to investor
    addOps = await processNewRelation({
        currenOps: [...ops, ...currentOps],
        spaceId: currSpaceId,
        entityOnGeo: entityOnGeo,
        fromEntityId: investor_geoId,
        toEntityId: normalizeToUUID(GEO_IDS.investorType),
        propertyId: SystemIds.TYPES_PROPERTY,
    });
    ops.push(...addOps.ops);

    // Add investors to project entity
    entityOnGeo = await searchEntity({
        entityId: project_geoId,
        spaceId: currSpaceId
    });
    addOps = await processNewRelation({
        currenOps: [...ops, ...currentOps],
        spaceId: currSpaceId,
        entityOnGeo: entityOnGeo,
        fromEntityId: project_geoId,
        toEntityId: investor_geoId,
        propertyId: propertyToIdMap['investor'],
        relationEntity: relationEntity,
    });
    ops.push(...addOps.ops);

    // add deals to investments relation entity
    entityOnGeo = await searchEntity({
        entityId: relationEntity,
        spaceId: currSpaceId
    });
    addOps = await processNewRelation({
        currenOps: [...ops, ...currentOps],
        spaceId: currSpaceId,
        entityOnGeo: entityOnGeo,
        fromEntityId: relationEntity,
        toEntityId: fundingRoundEntity,
        propertyId: propertyToIdMap['funding_rounds'],
    });
    ops.push(...addOps.ops);

    return { ops };
}

//export async function processInvestment(currentOps: Array<Op>, investmentId: string, teableClient: any): Promise<[Op | Op[], string]> {
export async function processInvestment({
  currentOps,
  investmentId,
  teableClient,
}: {
  currentOps: Array<Op>;
  investmentId: string;
  teableClient: any;
}): Promise<{
    ops: Op[]; id: string;
}> {

    // I can significantly speed this up if I sort by fundraising_name and then only run the bulk of this once, then just iterate over the different investors

    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let geoId: string;
    const propertiesSourced = [];
    
    // -------------------
    // ---- Pull data ---- 
    // -------------------
    await new Promise(resolve => setTimeout(resolve, 200));
    const page = await teableClient.get_record(PROD_TABLE_IDS.InvestmentRounds, investmentId)
    
    // -----------------------------
    // ---- Organize properties ---- 
    // -----------------------------
    // fields to pull from postgres

    const valueProperties = ['fundraising_name', 'round_date', 'raise_amount'];
    const relationProperties = ['funding_round']
    const values: { property: string; value: any }[] = [];

    const name = page?.fields?.['fundraising_name']

    if (geoId = await searchOps({
            ops: currentOps,
            property: SystemIds.NAME_PROPERTY,
            propType: "TEXT",
            searchText: name,
            typeId: normalizeToUUID(GEO_IDS.fundingRoundType)
        })) { 
        return { ops: (await addSpace(ops, currSpaceId)), id: geoId }
    } else {
        geoId = await searchEntities({
            spaceId: currSpaceId,
            property: SystemIds.NAME_PROPERTY,
            searchText: name,
            typeId: normalizeToUUID(GEO_IDS.fundingRoundType),
        });

        let entityOnGeo;
        if (!geoId) {
            geoId = Id.generate();
        } else {
            entityOnGeo = await searchEntity({
                entityId: geoId,
                spaceId: currSpaceId
            });
            console.log("entity exists on geo: ", geoId)
        }

        if (await hasBeenEdited(currentOps, geoId)) {
            return { ops: ops, id: geoId }
        } else {
            for (const property of valueProperties) {
                let start_val_len = values.length;
                
                const value = page?.fields?.[property];
                const includesCoincarp = typeof value === 'string' && value.toLowerCase().includes('coincarp.com');
                if ((property == "website_url") && (includesCoincarp)) {
                    console.error("website incldudes coincarp.com")
                } else {
                    // Only push property if it doesnt already exist on Geo
                    if (!(await valuePropertyExistsOnGeo(currSpaceId, entityOnGeo, propertyToIdMap[property]))) {
                        
                        if (page?.fields?.[property]) {
                            let value;
                            if (property == "round_date") {
                                const date = new Date(page?.fields?.[property]?.toString());
                                // Set to midnight UTC
                                date.setUTCHours(0, 0, 0, 0);
                                value =  date.toISOString();
                            } else {
                                value = page?.fields?.[property].toString()
                            }

                            values.push({
                                property: normalizeToUUID(propertyToIdMap[property]),
                                value: value,
                            });
                    }
                    }
                }
                if (values.length > start_val_len) {
                    propertiesSourced.push(property)
                }

            }
            if (values.length > 0) {
                addOps = Graph.createEntity({
                    id: geoId,
                    values: values
                })
                ops.push(...addOps.ops);

                if (propertiesSourced.length > 0) {
                    let source_id = GEO_IDS.coincarp
                    let source_url = page?.fields?.['source'].toString()

                    if (page?.fields?.['source'].toString().includes("coincarp.com")) {
                        addOps = await addSources({
                            currentOps: [...ops, ...currentOps],
                            entityId: geoId,
                            sourceEntityId: source_id,
                            propertiesSourced: propertiesSourced.map(property => propertyToIdMap[property]),
                            source_url: source_url,
                            source_db_id: undefined,
                            toEntity: undefined,
                        })
                        ops.push(...addOps.ops);
                        
                    }
                    propertiesSourced.length = 0
                }
            }

            //Add project type...
            addOps = await processNewRelation({
                currenOps: [...ops, ...currentOps],
                spaceId: currSpaceId,
                entityOnGeo: entityOnGeo,
                fromEntityId: geoId,
                toEntityId: GEO_IDS.fundingRoundType,
                propertyId: SystemIds.TYPES_PROPERTY,
            });
            ops.push(...addOps.ops);

            let toEntity;
            let projectEntityId;
            for (const property of relationProperties) {
                if (page?.fields?.[property]) {
                    if (property == 'funding_round') {
                        addOps = await processTag({
                            currentOps: [...ops, ...currentOps],
                            tagName: page?.fields?.[property],
                            tagType: GEO_IDS.fundingStageType
                        });
                    
                        ops.push(...addOps.ops);
                        toEntity = addOps.id;

                        addOps = await processNewRelation({
                            currenOps: [...ops, ...currentOps],
                            spaceId: currSpaceId,
                            entityOnGeo: entityOnGeo,
                            fromEntityId: geoId,
                            toEntityId: toEntity,
                            propertyId: propertyToIdMap[property],
                        });
                        ops.push(...addOps.ops);
                        
                        //ADD PROPERTIES SOURCED
                        propertiesSourced.push(property)
                        if (propertiesSourced.length > 0) {
                            let source_id = GEO_IDS.coincarp
                            let source_url = page?.fields?.['source'].toString()

                            if (page?.fields?.['source'].toString().includes("coincarp.com")) {
                                addOps = await addSources({
                                    currentOps: [...ops, ...currentOps],
                                    entityId: geoId,
                                    sourceEntityId: source_id,
                                    propertiesSourced: propertiesSourced.map(property => propertyToIdMap[property]),
                                    source_url: source_url,
                                    source_db_id: undefined,
                                    toEntity: toEntity,
                                })
                                ops.push(...addOps.ops);
                                
                            }
                            propertiesSourced.length = 0
                        }
                    }
                }
            }

            //Handle either an array of projects or single project
            const rawProjects = page?.fields?.['project'];
            const projectList = Array.isArray(rawProjects) ? rawProjects : rawProjects ? [rawProjects] : [];
            for (const project of projectList) {
                addOps = await processProject({
                    currentOps: [...ops, ...currentOps],
                    projectId: project?.id,
                    teableClient: teableClient
                });
                ops.push(...addOps.ops)

                projectEntityId = addOps.id
                toEntity = projectEntityId

                addOps = await processNewRelation({
                    currenOps: [...ops, ...currentOps],
                    spaceId: currSpaceId,
                    entityOnGeo: entityOnGeo,
                    fromEntityId: geoId,
                    toEntityId: toEntity,
                    propertyId: propertyToIdMap['project'],
                });
                ops.push(...addOps.ops);
                
                //ADD PROPERTIES SOURCED
                propertiesSourced.push('project')
                if (propertiesSourced.length > 0) {
                    let source_id = GEO_IDS.coincarp
                    let source_url = page?.fields?.['source'].toString()

                    if (page?.fields?.['source'].toString().includes("coincarp.com")) {
                        addOps = await addSources({
                            currentOps: [...ops, ...currentOps],
                            entityId: geoId,
                            sourceEntityId: source_id,
                            propertiesSourced: propertiesSourced.map(property => propertyToIdMap[property]),
                            source_url: source_url,
                            source_db_id: undefined,
                            toEntity: toEntity
                        })
                        ops.push(...addOps.ops);
                    }
                    propertiesSourced.length = 0
                }

                // Add investors
                let searchProperty = "fundraising_name"

                addOps = await iterateInvestors({
                    currentOps: ops,
                    teableClient: teableClient,
                    table_id: PROD_TABLE_IDS.InvestmentRounds,
                    property_value: page?.fields?.[searchProperty].toString(),
                    search_property: searchProperty,
                    projectEntityId: projectEntityId,
                    fundraisingRoundEntityId: geoId,
                });
                ops.push(...addOps.ops)
            }

            return { ops: (await addSpace(ops, currSpaceId)), id: geoId }
        }
    }
}


