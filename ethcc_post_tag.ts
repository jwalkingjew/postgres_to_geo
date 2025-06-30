import { PROD_TABLE_IDS } from "./src/teable-meta";
import { searchEntities, searchEntity, hasBeenEdited, searchOps, normalizeToUUID, propertyToIdMap, relationPropertyExistsOnGeo, valuePropertyExistsOnGeo, processNewRelation, GEO_IDS,  addSpace } from "./src/constants_v2";

import { type DataType,  type Op } from "@graphprotocol/grc-20";
import { Graph, Position, Id, Ipfs, SystemIds } from "@graphprotocol/grc-20";



//export async function processTag(currentOps: Array<Op>, tagName: string, tagType: string): Promise<Data> {
export async function processTag({
  currentOps,
  tagName,
  tagType,
}: {
  currentOps: Array<Op>;
  tagName: string;
  tagType: string;
}): Promise<{
    ops: Op[]; id: string;
}> {

    //TODO - Update this to handle arrays of tagTypes

    const ops: Array<Op> = [];
    const currSpaceId = GEO_IDS.cryptoSpace;
    let addOps;
    let geoId: string;
    
    // -----------------------------
    // ---- Organize properties ---- 
    // -----------------------------
    // fields to pull from postgres

    const valueProperties = ['name'];
    const values: { property: string; value: any }[] = [];
    const name = tagName

    if (geoId = await searchOps({
            ops: currentOps,
            property: SystemIds.NAME_PROPERTY,
            propType: "TEXT",
            searchText: name,
            typeId: normalizeToUUID(tagType)
        })) { 
        return { ops: ops, id: geoId }
    } else {
        geoId = await searchEntities({
            spaceId: currSpaceId,
            property: SystemIds.NAME_PROPERTY,
            searchText: name,
            //typeId: tagType,
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

        //if ((entityOnGeo) || (await hasBeenEdited(currentOps, geoId))) {
        if ((await hasBeenEdited(currentOps, geoId))) {
            return { ops: ops, id: geoId }
        } else {
            addOps = Graph.createEntity({
                id: geoId,
                values: [
                    {
                        property: SystemIds.NAME_PROPERTY,
                        value: name
                    }
                ]
            })
            ops.push(...addOps.ops);
        
            // Add appropriate type to tag
            addOps = await processNewRelation({
                currenOps: [...ops, ...currentOps],
                spaceId: currSpaceId,
                entityOnGeo: entityOnGeo,
                fromEntityId: geoId,
                toEntityId: normalizeToUUID(tagType),
                propertyId: SystemIds.TYPES_PROPERTY,
            });
            ops.push(...addOps.ops);

            return { ops: (await addSpace(ops, currSpaceId)), id: geoId }
        }
    }
}


