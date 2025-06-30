const { Client } = require("@notionhq/client")
import { SystemIds, Triple } from '@graphprotocol/grc-20';
import { searchEntity } from './search_entity';
import { walletAddress, TABLES, mainnetWalletAddress } from './src/constants';
import * as fs from "fs";
import { publish } from './src/publish';


// Initializing a client
const notion = new Client({
  auth: process.env.NOTION_TOKEN,
})

let geo_ids;
let position;
const ops: Array<Op> = [];
let addOps;
let space = "LHDnAidYUSBJuvq7wDPRQZ"

//Day 1
//geo_ids = ["Enh9fAYip2rRq2DcUAYqMQ", "3MwY8WftKXZBqVxjKfb6HY", "CTVU18FZNTQmgJaV6P1KQd", "HxzirGCz9HsmLkWCMCLNeU", "SHCY1STBC7V4kTGKj5Mc2z", "E7arotHh8k9jiuAMMgVqeS", "VVSNGqYXnN6LQhudLBePGZ", "6YzjN9d1rH1PHNmtVfZZvm", "BZGW4dY3w4QX6mGVqL2Y2Z", "Wkz1yKUT4dfrsfuwPaTJ8q", "NQDrhBwqvwTexunb36jGuk", "Agg8GotUxM7bVvSdV6LMpj", "9AoiZhAkYchRGF9Fw924a4", "GcGbdqVeMheRjKpxLgSkUW", "84qUDxBUatPm8QigAzEWUC"]
//geo_ids = ["AaZV8XjwLEnvNq8fp2Jx9P", "8Gyq8jsiL8hKszCtgLQhSL", "Ksgi934EeFZoWZFEi47pTc", "233vb9st5ZKYoSwKy9NrSm", "6JtdBUpyfbHeP4yYQmxTeK", "Ua1WqCkF7tp26TGRD1mvDd", "9D572CN6RLeU7tzrpDvWX8", "MVDPUkp1Vy43Jzd95bg4WH", "9DQjuJWufanCk8sbcWoDjK", "85Q4tVzYFjdwy1BwX3d82w"]

//Day 2
//geo_ids = ["JNPGp8ndGisEGSbcngXWxk", "HNkEE7u83qRUwdtYkyqRGt", "7hrzuwcMzHRW57xsvyVN8i", "UDvhRvYjNow2tC1wMKFE8D", "TuPsQHyXDZtvhLALTKH1KK", "8Pii4CitCWybnoWWYkTYkf", "Gpvhva1VoZTEtHeYnpbPHW", "Xbicus1gBVppYXrbQxdA99", "HTKQZCyVS6Rt89Zd1GXUT1", "FRCYZhLUimo3GudtZoJvkF", "JP9TCEWzNXmFcijWKWEnTL", "Uxt7Gjar2JNsidfG1NDeVz", "Twk7EptR4aFNN9DJMJB7Yk", "GiwGsboUFpFyXQaVF3K8Jb"]
//geo_ids = ["BZSJVvackooHEYLaXxAs9c", "P3DD1XL86iK8UCqYFaQ4e4", "GD8Ndio92RiAdPk3XFuVYa", "P5vcXLdpmpmhB237RHyq35", "LdbCLkFRPED8PQrBriukPh", "BBimcVGuqhLskVYLF8bPes", "EsZGf19Dmo9kZ9EtS8Bbms", "WyU1ksjCmse22PYijR3DcX", "2fupDUvM5ziuc3W627e567", "CHnT3wHNQqTUfzMSGQdM3a", "PfcoLWrMcvaqYvrvspWP1M", "MGXzUehd2L3oQQLRztYveE", "9NAWr3p9UE6XwhHfvaRcPH"]
//geo_ids = ["DTohy4fnppPUKE6cLbbbwC", "KiofPMkdJtcSDDKcqAbEWC", "5YBTEtDTNChfuvaisrH6iL", "SmcD8MMiXSPiBFk73iLWdt", "WCmfNNDGsZTR7Vuoig1Pxd", "Fcc7vzma5skeJmiN96u9NY", "Sk6dXUmnxup3C4uBQCdNqv", "88gi5jtRmSTfaZvKAdbwJ1", "Mb34mEMKqNgzQginfSSRYu", "LmutT5Uea3LsBpow48PFPe", "RNY3vHRxH9twrDGmtqEN5G", "DAz9z9C43jGX9ij2BxMKbU", "D4vy1ESzC6PZK7uL68hv5Z", "6bxxcAv8ZUtQENUZNvEcRw", "XqKBsBN1Eq9v6WrBMdnHhQ", "GqpFiBnTpHF8tpDzqvQtFc"]
//geo_ids = ["Vu7aNY7rNTnFcinjjhKFnt", "RRBJvJGfBQNFXdKYuq3s8n", "F57x7E4gQ49ZvWpHKCEPcK", "PaShtjQ8M1qvF4PaCzyoz1", "F3LPH2hUGTpvRfeeRBhC3Y", "Mm2QiGjwZet46id3vASHNe", "8AzMgRS21dn5a59opnfr7k", "7Bxy5fR5idsHt3iaQA93r2", "QXZQoTvuCDkhx2j7kqnRPv"]

//Day 3
//geo_ids = ["XR5N89vnJAULCqE5s64cjC", "Seb6eKEej59YMrqjKzC5Uy", "4Ny1M1tqEk6Q73qCXQkH2k", "CW9ZfeUKjUpsrBRabDUZR6", "NvzuonBRSnDEUuh69d7gNF", "Xgw342oqJjcCHhyqHjPg1i", "EG3J7hW9gqgZpFEAfdtD4F", "4v9UXXVmNPLGA89Z4N9PWZ", "E4wnXeW2Mu8BnzemFv8sbR", "Wykad4C3KNdTzqPMcJuyeL", "QnAwYn15UShDAC3ecr6qu9", "4QjhxcprmLHPFYt9fdRP8u"]
geo_ids = ["RmV8n4PAxiKg4TkvznXGra", "3pBvWn3XJFARSYADGZ2sjd", "5WoR5bD2Rw4YAjdGrfqF5K", "ECYovQncLNBVDVrr3K7pXL", "5NySesunma8GhHZThJbtqn", "EaSvsgtyf6iUADeQrB8AvU", "UBzXn8YDbgeuwWEM8ULf84", "UyxXUd7RDg9gBS49htR88J", "KWSJrTQJeH92K6MAXeCaa7", "RruCFBohJCNebdAyAyeebm", "32FfxN4eqmpKmJ7RXfADnH", "FSvaJsRQS3qZtCGzznWoqJ", "N1FeWoQ8GfwBcNixrAF3WS", "CFrKBWc7qZiwbCWEhe9TSk"]
//geo_ids = ["6hNJUsQffj5PZJ72eVczfC", "Rqo1Q1TEPKjwJRuRApngi9", "UNAp9aHzQjskznuzXuKmyX", "3GqZFn28LUBxCzYbKbWDZ3", "SEbexfz8bFe2WFUx4VXwhS", "F3sYvTMgVvGkbJeEcPN1WJ", "7yohq5hvUaz5DgD1skycfN", "8jmAXa3E2juMWD85zcovM6"]

//Order tabs
//geo_ids = ["9zEPNCCCa4sEP9c1hXnotq", "WxJ25GLfyXD7p1seTmGLJP", "9gDUv9svTcX48ZizGrh2mw", "WTaijx5JaeQABgkuyrvyeV", "8yGhw1f4cS6Vry49EyHCqJ"]

//Order properties
//geo_ids = ["G7n3CefogPmsaoJ1FvBUrE", "VsGU5wawdu5foYG9F5KCBd", "CMoLjKG7QW9m8rFHiSKNuc", "4wpMrGDPF4ujAq5exeKXRY"]

let charCode = "A".charCodeAt(0);

for (let i = 0; i < geo_ids.length; i++) {
  const letter = String.fromCharCode(charCode + i);
  position = `7LsJcXFB.${letter}`;
  addOps = Triple.make({
    attributeId: 'WNopXUYxsSsE51gkJGWghe',
    entityId: geo_ids[i],
    value: {
      type: 'TEXT',
      value: position,
    },
  });
  ops.push(addOps);

}
const txHash = await publish({
    spaceId: space,
    author: mainnetWalletAddress,
    editName: `Update indexes for day 1 workshop`,
    ops: ops, // An edit accepts an array of Ops
}, "MAINNET");


console.log(`Your transaction hash for ${space} is:`, txHash);
