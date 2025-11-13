import { GEO_IDS } from './src/constants_v2.ts';
import { Graph, IdUtils, SystemIds, type Op } from '@graphprotocol/grc-20';
import PostgreSQLClient from "./src/postgres-client.ts";
import { topicBreakdown, platformBreakdown, sourceBreakdown, personBreakdown, listenOnBreakdown, podcastBreakdown, roleBreakdown, podcastAppearanceBreakdown, episodeBreakdown, quoteBreakdown, claimBreakdown, read_in_tables, pageBreakdown, textBlockBreakdown, loadGeoEntities } from './src/setup_ontology.ts';
import { type Entity, entityCache, buildEntityCached } from './src/inputs.ts';
import { processEntity } from './post_entity.ts';
import { printOps, publishOps } from "./src/functions.ts";

const ops: Array<Op> = [];
let addOps;
const processingCache: Record<string, Entity> = {};

const offset = 0
const limit = 100
const pgClient = new PostgreSQLClient();
// global or passed down as a parameter

console.log(episodeBreakdown)

console.log("HERE")
process.on('SIGINT', async () => {
    console.log("FINAL OFFSET: ", offset);
    console.log('Exiting gracefully...');
    await pgClient.close();
    process.exit(0);
});

try {
    const geoEntities = await loadGeoEntities()
    
    let tables = await read_in_tables({
        pgClient: pgClient,
        offset: offset,
        limit: limit
    });

    episodeBreakdown.relations = episodeBreakdown.relations.filter(
        r => !['notable_claims', 'notable_quotes'].includes(r.type)
    );
    console.log(episodeBreakdown)
    
    
    const formattedEpisodes = tables.episodes.map(p =>
        buildEntityCached(p, episodeBreakdown, GEO_IDS.podcastsSpace, tables, geoEntities, entityCache)
    );
    // Convert to formatted JSON string
    //const episodes_print = JSON.stringify(formattedEpisodes, null, 2); // 2-space indentation
    //fs.writeFileSync("formattedEpisodes.txt", episodes_print, "utf-8");
    //console.log(geoEntities.podcasts[0])

    for (const episode of formattedEpisodes) {
        addOps = await processEntity({
            currentOps: ops,
            processingCache: processingCache,
            entity: episode
        })
        ops.push(...addOps.ops)
    }

    printOps(ops, "test_push_podcast_allin_10ep.txt")
    await publishOps(ops)
    
    /*

    const formattedClaims = tables.claims.map(p =>
        buildEntityCached(p, claimBreakdown, GEO_IDS.podcastsSpace, tables, geoEntities, entityCache)
    );
    

    for (const claim of formattedClaims) {
        addOps = await processEntity({
            currentOps: ops,
            processingCache: processingCache,
            entity: claim
        })
        ops.push(...addOps.ops)
    }

    printOps(ops, "test_push_claims_rerun.txt")
    //await publishOps(ops)
  
    /*
    //PRINT ALL TABLES and INDIVIDUAL EPISODE
    //console.log(tables.listen_on_links)
    console.log("TABLES OUTPUT")
    
    const formattedPodcasts = tables.podcasts.map(p =>
        buildEntityCached(p, podcastBreakdown, GEO_IDS.podcastsSpace, tables, geoEntities, entityCache)
    );

    console.log(formattedPodcasts)


    // Convert to formatted JSON string
    const text2 = JSON.stringify(geoEntities.podcasts, null, 2); // 2-space indentation
    //fs.writeFileSync("geoEntitiesPodcasts.txt", text2, "utf-8");
    //console.log(geoEntities.podcasts[0])

    
    
    for (const podcast of formattedPodcasts) {
        addOps = await processEntity({
            currentOps: ops,
            processingCache: processingCache,
            entity: podcast
        })
        ops.push(...addOps.ops)
    }
    console.log(ops)
    

    
    const formattedEpisodes = tables.episodes.map(p =>
        buildEntityCached(p, episodeBreakdown, GEO_IDS.podcastsSpace, tables, geoEntities, entityCache)
    );
    // Convert to formatted JSON string
    const text = JSON.stringify(formattedEpisodes, null, 2); // 2-space indentation
    fs.writeFileSync("formattedEpisodes.txt", text, "utf-8");

    

    for (const episode of formattedEpisodes) {
        addOps = await processEntity({
            currentOps: ops,
            processingCache: processingCache,
            entity: episode
        })
        ops.push(...addOps.ops)
    }
    
    //console.log(processingCache)
    printOps(ops, "test_push_podcast_06.txt")
    
    await publishOps(ops)
    /*
    const formattedPodcasts = tables.podcasts.map(p =>
        buildEntity(p, podcastBreakdown, GEO_IDS.podcastsSpace, tables, geoEntities)
    );
    console.log(formattedPodcasts[0].relations)
    const formattedEpisodes = tables.episodes.map(p =>
        buildEntity(p, episodeBreakdown, GEO_IDS.podcastsSpace, tables, geoEntities)
    );
    console.log(formattedEpisodes[0].relations)
    */

} catch (error) {
    console.error(error);
} finally {
    await pgClient.close();
}
