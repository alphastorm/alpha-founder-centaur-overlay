WORKFLOW_NAME = "alpha_founder_qualification_attestation"
PROMPT = (
    "Use the alpha-founder-qualification skill and the qualification_attestor tool "
    "to attest the supplied public nonce. Return only the tool's JSON result."
)


async def handler(inp, ctx):
    nonce = str(inp.get("nonce", ""))
    if not nonce:
        raise ValueError("nonce is required")
    return await ctx.call_tool("qualification_attestor", "attest", {"nonce": nonce})
