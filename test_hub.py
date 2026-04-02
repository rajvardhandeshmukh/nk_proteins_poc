import agentic_hub

def run_test(q):
    print(f"\n==== Testing: {q} ====")
    res = agentic_hub.ask_agentic(q, "meta-llama/Llama-3.3-70B-Instruct-Turbo", "together")
    if isinstance(res, dict):
        print("Intent:", res.get("intent"))
        print("SQL:", res.get("sql"))
        print("Rows:", res.get("rows"))
        print("Narrative:", res.get("narrative"))
    else:
        print("Result:", res)

run_test("What was the total revenue for the West region last year?")
run_test("Forecast revenue for the next 3 months.")
run_test("How are the sales for FreshMart Vadodara looking?")
run_test("Segment my customers by profitability.")
run_test("Give me the biggest risks across the business right now.")
