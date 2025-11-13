import { SystemIds, type Op, Graph, Id } from "@graphprotocol/grc-20";
import { testnetWalletAddress } from "./src/constants_v2";

const id = await Graph.createSpace({
    editorAddress: testnetWalletAddress,
    name: "Podcasts",
    network: "TESTNET"
})
console.log(id)