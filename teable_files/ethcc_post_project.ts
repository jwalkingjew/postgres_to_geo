import { PROD_TABLE_IDS } from "./src/teable-meta";
import { searchEntities, searchEntity, hasBeenEdited, searchOps, normalizeToUUID, propertyToIdMap, relationPropertyExistsOnGeo, valuePropertyExistsOnGeo, processNewRelation, GEO_IDS,  addSpace, addSources, propertyToTypeIdMap } from "./src/constants_v2";

import { type DataType,  type Op } from "@graphprotocol/grc-20";
import { Graph, Position, Id, Ipfs, SystemIds } from "@graphprotocol/grc-20";
import { processTag } from "./ethcc_post_tag";

async function addProjTags({
  currentOps,
  teableClient,
  page,
  tagProperties,
  projectEntityId,
  entityOnGeo,
}: {
  currentOps: Array<Op>;
  teableClient: any;
  page: any;
  tagProperties: string[];
  projectEntityId: string;
  entityOnGeo: any;
}): Promise<{
    ops: Op[];
}> {
    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let geoId = projectEntityId;
    let toEntity;
    const propertiesSourced: string[] = [];


    

    for (const property of tagProperties) {
        const tagEntries = page?.fields?.[property];
        const tagList = Array.isArray(tagEntries) ? tagEntries : tagEntries ? [tagEntries] : [];
        for (const tag of tagList) {
            const tagPage = await teableClient.get_record(PROD_TABLE_IDS.Tags, tag.id)
            addOps = await processTag({
                currentOps: [...ops, ...currentOps],
                tagName: tagPage?.fields?.['name'],
                tagType: propertyToTypeIdMap[property]
            });
            ops.push(...addOps.ops)
            toEntity = addOps.id

            addOps = await processNewRelation({
                currenOps: [...ops, ...currentOps],
                spaceId: currSpaceId,
                entityOnGeo: entityOnGeo,
                fromEntityId: geoId,
                toEntityId: toEntity,
                propertyId: propertyToIdMap[property],
            });
            ops.push(...addOps.ops);
            //TODO - ADD SOURCING
        }
    }
    return { ops: (await addSpace(ops, currSpaceId)) }
}


//export async function processProject(currentOps: Array<Op>, projectId: string, teableClient: any): Promise<[Op | Op[], string]> {
export async function processProject({
  currentOps,
  projectId,
  teableClient,
}: {
  currentOps: Array<Op>;
  projectId: string;
  teableClient: any;
}): Promise<{
    ops: Op[]; id: string;
}> {
    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let geoId: string;
    
    // -------------------
    // ---- Pull data ---- 
    // -------------------
    //console.log("PROJECT ID: ", projectId)
    await new Promise(resolve => setTimeout(resolve, 200));
    const page = await teableClient.get_record(PROD_TABLE_IDS.Projects, projectId)
    
    // -----------------------------
    // ---- Organize properties ---- 
    // -----------------------------
    // fields to pull from postgres

    const valueProperties = ['name', 'description', 'website_url', 'x_url', 'date_founded', 'linkedin_url'];
    const relationProperties = ['cover', 'logo', 'project_type']
    const values: { property: string; value: any }[] = [];

    const name = page?.fields?.['name']

    if (geoId = await searchOps({
            ops: currentOps,
            property: SystemIds.NAME_PROPERTY,
            propType: "TEXT",
            searchText: name,
            typeId: SystemIds.PROJECT_TYPE
        })) { 
        return { ops: ops, id: geoId }
    } else {
        geoId = await searchEntities({
            spaceId: currSpaceId,
            property: SystemIds.NAME_PROPERTY,
            searchText: name,
            typeId: SystemIds.PROJECT_TYPE,
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

        //Limit publishing to only if includes some set of property values
        //if ((!entityOnGeo) && ((name == "NONE") || ((desc == "NONE") && (avatar_url == "NONE")))) {
        //    return [ops, null]
        //}

        if (await hasBeenEdited(currentOps, geoId)) {
            return { ops: ops, id: geoId }
        } else {
            for (const property of valueProperties) {
                const value = page?.fields?.[property];
                const includesCoincarp = typeof value === 'string' && value.toLowerCase().includes('coincarp.com');
                if ((property == "website_url") && (includesCoincarp)) {
                    console.error("website incldudes coincarp.com")
                } else {
                    // Only push property if it doesnt already exist on Geo
                    if (!(await valuePropertyExistsOnGeo(currSpaceId, entityOnGeo, propertyToIdMap[property]))) {
                        if (page?.fields?.[property]) {
                            let value;
                            if (property == "date_founded") {
                                const date = new Date(page?.fields?.[property].toString());
                                // Set to midnight UTC
                                date.setUTCHours(0, 0, 0, 0);
                                value =  date.toISOString();
                            } else {
                                value = page?.fields?.[property].toString()
                            }
                            values.push({
                                property: normalizeToUUID(propertyToIdMap[property]),
                                value: value
                            });
                        }
                    }
                }
            }
            if (values.length > 0) {
                addOps = Graph.createEntity({
                    id: geoId,
                    values: values
                })
                ops.push(...addOps.ops);
            }

            //Add project type...
            addOps = await processNewRelation({
                currenOps: [...ops, ...currentOps],
                spaceId: currSpaceId,
                entityOnGeo: entityOnGeo,
                fromEntityId: geoId,
                toEntityId: GEO_IDS.project_type_id,
                propertyId: SystemIds.TYPES_PROPERTY,
            });
            ops.push(...addOps.ops);

            //Write cover ops
            const imageProperties = ['cover', 'logo'];
            for (const imageProp of imageProperties) {
                if (page?.fields?.[imageProp]) {
                    if (!(await relationPropertyExistsOnGeo(currSpaceId, entityOnGeo, propertyToIdMap[imageProp]))) {
                        // create an image
                        //const { id: imageId, ops: createImageOps } = await Graph.createImage({
                        //    url: page?.fields?.[imageProp],
                        //});
                        //ops.push(...createImageOps)
                        console.log(`${imageProp} IMAGE NOT CREATED PROPERLY FOR `, geoId);
                        const imageId = Id.generate()

                        addOps = await processNewRelation({
                            currenOps: [...ops, ...currentOps],
                            spaceId: currSpaceId,
                            entityOnGeo: entityOnGeo,
                            fromEntityId: geoId,
                            toEntityId: imageId,
                            propertyId: propertyToIdMap[imageProp],
                        });
                        ops.push(...addOps.ops);
                    }
                }
            }

            //const tagProperties = ['related_industries', 'related_services_or_products', 'related_asset_categories', 'related_technologies', 'tags']
            //addOps = await addProjTags([...ops, ...currentOps], teableClient, page, tagProperties, geoId, entityOnGeo)
            //ops.push(...addOps.ops);

            return { ops: (await addSpace(ops, currSpaceId)), id: geoId }
        }
    }
}


