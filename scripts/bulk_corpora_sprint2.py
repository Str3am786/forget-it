"""S4 — grow the deterministic corpora to n ≥ 150 per stage (+ hard negatives).

Writes *_bulk2.yaml SIBLINGS (ids continue after the sprint-1 bulk; exemplar
and bulk files untouched; every fixture verified: false):

  corpus_e  +100 → 150 runnable   (same classes, same independent simulator)
  corpus_r  +110 deterministic → 150 deterministic (+10 dense unchanged)
  verdict   +120 deterministic tiers → 150 deterministic (30 judge-tier unchanged)
  reader    +15 hard negatives: related_but_insufficient_abstain (the D6-purge
            bluffing mode: evidence rich in adjacent specifics, answer absent)

Class-logic guards are asserted at generation time exactly as in sprint 1
(e.g. every exact_match pair must be normalization-equal; every hallucination
answer must NOT contain its reference; every nonce is unique corpus-wide).

Deterministic (seed 20260710). Rerun: python scripts/bulk_corpora_sprint2.py
"""
from __future__ import annotations

import random
from pathlib import Path

import yaml

from bulk_corpora import simulate, MIN_COMPRESS_BYTES, MIN_SAVING_RATIO
from bulk_corpora_data import DISTRACTORS, _NONCES_BM25, _NONCES_GHOST

import sys
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
from forgetit.benchmark.grading import contains_match, normalized_em  # noqa: E402

CORP = REPO / "golden_corpora"
SEED = 20260710


# ══ corpus_e (+100) — reuses the sprint-1 independent simulator ═══════

def gen_corpus_e2(rng: random.Random) -> list[dict]:
    out: list[dict] = []
    nid = 51
    names = "ABCDEFGH"

    def emit(cls, inp, expected, rationale, assumptions):
        nonlocal nid
        out.append({"id": f"e{nid:03d}", "class": cls, "verified": False,
                    "assumptions": assumptions, "input": inp,
                    "expected": expected, "rationale": rationale})
        nid += 1

    made = 0                                     # lru_touch ×20
    while made < 20:
        n = rng.choice([3, 4, 5, 6])
        size = rng.choice([25, 30, 40, 50])
        budget = size * (n - 1) + size // 2
        events = [{"op": "insert", "item": names[i], "size": size} for i in range(n - 1)]
        touched = rng.sample(range(max(1, n - 2)), k=min(rng.choice([1, 2]), max(1, n - 2)))
        for t in sorted(touched):
            events.append({"op": "touch", "item": names[t]})
        events.append({"op": "insert", "item": names[n - 1], "size": size})
        exp = simulate("lru", budget, events)
        blind = simulate("lru", budget, [e for e in events if e["op"] != "touch"])
        if exp["evictions"] == blind["evictions"] or not exp["evictions"]:
            continue
        emit("lru_touch", {"policy": "lru", "budget": budget, "events": events}, exp,
             f"Touches on {sorted(names[t] for t in touched)} flip the victim(s) from "
             f"{blind['evictions']} to {exp['evictions']} — live recency bookkeeping.",
             ["touch updates recency", "eviction loop runs inside insert only"])
        made += 1

    made = 0                                     # lru_multi_evict_loop ×15
    while made < 15:
        big = rng.choice([60, 70, 80, 90])
        small = rng.choice([15, 20, 30])
        budget = big + small + rng.choice([0, 5, 10])
        events = [{"op": "insert", "item": "A", "size": big},
                  {"op": "insert", "item": "B", "size": small},
                  {"op": "insert", "item": "C", "size": big},
                  {"op": "insert", "item": "D", "size": big}]
        rng.shuffle(events)
        for i, e in enumerate(events):
            e["item"] = names[i]                 # relabel post-shuffle: unique ids
        exp = simulate("lru", budget, events)
        if len(exp["evictions"]) < 2:
            continue
        emit("lru_multi_evict_loop", {"policy": "lru", "budget": budget, "events": events}, exp,
             f"Sizes {[e['size'] for e in events]} under budget {budget}: the loop fires "
             f"repeatedly ({exp['evictions']}), re-reading recency between evictions.",
             ["loop evicts repeatedly until total <= budget"])
        made += 1

    for _ in range(10):                          # lfu_tiebreak ×10
        n = rng.choice([3, 4, 5])
        size = rng.choice([30, 35, 40])
        budget = size * (n - 1) + size // 2
        events = [{"op": "insert", "item": names[i], "size": size} for i in range(n)]
        exp = simulate("lfu", budget, events)
        emit("lfu_tiebreak", {"policy": "lfu", "budget": budget, "events": events}, exp,
             f"All freqs 0 → tie-break = insertion order → {exp['evictions']}.",
             ["freq counts touches only", "tie-break = insertion order"])

    made = 0                                     # lfu_freq ×10
    while made < 10:
        size = rng.choice([30, 40, 50])
        n_res = rng.choice([2, 3])
        budget = n_res * size + size // 2
        events = [{"op": "insert", "item": names[i], "size": size} for i in range(n_res)]
        heavy = rng.randrange(n_res)
        events += [{"op": "touch", "item": names[heavy]}] * rng.choice([2, 3])
        light = rng.choice([i for i in range(n_res) if i != heavy])
        events += [{"op": "touch", "item": names[light]}]
        events += [{"op": "insert", "item": names[n_res], "size": size}]
        exp = simulate("lfu", budget, events)
        lru_exp = simulate("lru", budget, events)
        if exp["evictions"] == lru_exp["evictions"] or not exp["evictions"]:
            continue
        emit("lfu_freq", {"policy": "lfu", "budget": budget, "events": events}, exp,
             f"LFU evicts {exp['evictions']} where LRU would evict {lru_exp['evictions']} — "
             "frequency beats recency. Incoming item EXEMPT (Q1 ruling).",
             ["freq counts touches only", "incoming item EXEMPT from eviction (Q1)"])
        made += 1

    made = 0                                     # fifo ×15
    while made < 15:
        n = rng.choice([4, 5, 6])
        size = rng.choice([25, 30, 40])
        budget = size * (n - 2) + size // 2
        events = [{"op": "insert", "item": names[i], "size": size} for i in range(n - 2)]
        events.append({"op": "touch", "item": names[rng.randrange(max(1, n - 3))]})
        events.append({"op": "insert", "item": names[n - 2], "size": size})
        events.append({"op": "insert", "item": names[n - 1], "size": size})
        exp = simulate("fifo", budget, events)
        lru_exp = simulate("lru", budget, events)
        if exp["evictions"] == lru_exp["evictions"]:
            continue
        emit("fifo", {"policy": "fifo", "budget": budget, "events": events}, exp,
             f"FIFO evicts insertion-order {exp['evictions']} despite the touch "
             f"(LRU would evict {lru_exp['evictions']}).",
             ["eviction order == insertion order, touches ignored"])
        made += 1

    for i in range(15):                          # size_policy_arith ×15
        pol = "keep_smallest" if i < 10 else "evict_smallest"
        sizes = rng.sample([10, 15, 20, 35, 45, 55, 65, 75], k=3)
        incoming = sorted(sizes)[1]
        first_two = [s for s in sizes if s != incoming][:2]
        budget = sum(first_two) + incoming - 5
        events = [{"op": "insert", "item": "A", "size": first_two[0]},
                  {"op": "insert", "item": "B", "size": first_two[1]},
                  {"op": "insert", "item": "C", "size": incoming}]
        exp = simulate(pol, budget, events)
        emit("size_policy_arith", {"policy": pol, "budget": budget, "events": events}, exp,
             f"{pol}: victim is the {'largest' if pol == 'keep_smallest' else 'smallest'} "
             f"resident of {first_two} → {exp['evictions']}; incoming mid-sized "
             "(eligibility-robust).",
             [f"{pol} victim rule", "incoming-eligibility-robust by construction"])

    for _ in range(10):                          # scripted_eviction ×10
        n_items = rng.choice([3, 4, 5])
        step = n_items + 1
        victims = rng.sample([names[j] for j in range(n_items)], k=rng.choice([1, 2]))
        events = [{"op": "insert", "item": names[j], "size": 40} for j in range(n_items)]
        events.append({"op": "step"})
        exp = simulate("scripted_eviction", 10 ** 6, events, script={step: victims})
        emit("scripted_eviction",
             {"policy": "scripted_eviction", "script": {"evict_at_step": {step: victims}},
              "budget": 10 ** 6, "events": events}, exp,
             f"Fiat eviction of {victims} at step {step}; budget never triggers.",
             ["scripted_eviction policy exists", "budget never triggers"])

    comp_cases = [                               # compression_trigger ×5
        ([280, 280, 280], 620, 40, [], "compress-not-evict"),
        ([320, 320, 320], 700, 35, ["B"], "LFU selector skips the touched item"),
        ([160, 320, 240], 420, 30, [], "sub-floor evict then compress"),
        ([300, 300, 300], 640, 190, [], "reject on saving < 0.5 → evict"),
        ([250, 250, 950], 720, 30, [], "two compressions in one activation"),
    ]
    for sizes, budget, stub, touches, tag in comp_cases:
        events = []
        for j, s in enumerate(sizes[:-1]):
            events.append({"op": "insert", "item": names[j], "size": s})
        for t in touches:
            events.append({"op": "touch", "item": t})
        events.append({"op": "insert", "item": names[len(sizes) - 1], "size": sizes[-1]})
        exp = simulate("compression", budget, events, stub_chars=stub)
        emit("compression_trigger",
             {"policy": "compression", "stub_summary_chars": stub,
              "budget": budget, "events": events}, exp,
             f"{tag}: floor {MIN_COMPRESS_BYTES}B, ratio {MIN_SAVING_RATIO}; expected "
             f"compressions={exp['compressions']} evictions={exp['evictions']}.",
             [f"min_compress_bytes={MIN_COMPRESS_BYTES}", f"min_saving_ratio={MIN_SAVING_RATIO}",
              "stub summariser (plumbing corpus)"])

    assert len(out) == 100, len(out)
    return out


# ══ corpus_r (+110 deterministic) ═════════════════════════════════════

_ONSETS = ["br", "cl", "dr", "fl", "gr", "kr", "pl", "sk", "tr", "vr", "zw", "thr", "sn", "qu"]
_MIDS = ["av", "el", "ib", "oq", "ur", "an", "op", "ex", "il", "um"]
_TAILS = ["ine", "ax", "oid", "une", "eth", "arn", "yx", "ule", "ost", "amp"]


def _fresh_nonces(rng: random.Random, n: int, taken: set[str]) -> list[str]:
    out: list[str] = []
    while len(out) < n:
        w = rng.choice(_ONSETS) + rng.choice(_MIDS) + rng.choice(_ONSETS) + rng.choice(_TAILS)
        if w not in taken and len(w) >= 8:
            taken.add(w)
            out.append(w)
    return out


_COMMONS = ["bench", "carpet", "mirror", "bucket", "teapot", "cabinet", "stroller",
            "satchel", "trellis", "dinghy", "easel", "bellows", "hammock", "quilt",
            "tricycle", "barrow", "kennel", "sundial", "harp", "anvil", "plinth",
            "gazebo", "lectern", "spinet", "churn"]
_MIDWORDS = ["walnut", "crimson", "wicker", "pewter", "indigo", "linen", "cobalt",
             "maple", "slate", "amber", "birch", "russet", "ochre", "teak", "sage",
             "umber", "sable", "flax", "onyx", "rowan", "aspen", "coral", "basalt",
             "sienna", "madder"]
_RAREWORDS = ["finial", "ferrule", "gudgeon", "trunnion", "escutcheon", "spandrel",
              "newel", "mullion", "voussoir", "cotter", "pintle", "tang", "swage",
              "chamfer", "kerf", "rabbet", "dado", "spline", "gusset", "cleat",
              "batten", "purlin", "corbel", "boss", "quoin"]

_PROCEDURES2 = [("pottery glazing", 6), ("sail rigging", 7), ("hive splitting", 8),
                ("lens polishing", 6), ("barrel coopering", 7), ("fresco preparation", 8),
                ("violin varnishing", 6), ("net mending", 7), ("kite bridling", 8),
                ("scythe peening", 6), ("bell tuning", 7), ("saddle fitting", 8),
                ("press proofing", 6), ("dye mordanting", 7), ("wheel truing", 8),
                ("forge tempering", 6), ("map engraving", 7), ("rope splicing", 8),
                ("clock escapement adjustment", 6), ("stained glass leading", 7),
                ("bonsai wiring", 8), ("falconry imping", 6), ("tapestry warping", 7),
                ("canoe gunwale replacement", 8), ("mill dressing", 6)]

_ACTIONS2 = ["prepare the surface", "check the tension", "warm the medium",
             "mark the reference line", "clean the joints", "confirm the fit",
             "secure the clamps", "record the measurement"]


def gen_corpus_r2(rng: random.Random) -> list[dict]:
    out: list[dict] = []
    nid = 51
    taken = set(_NONCES_BM25) | set(_NONCES_GHOST) | {"zyqbrontide", "vorplex"}

    def emit(cls, retriever, inp, expected, rationale, assumptions):
        nonlocal nid
        out.append({"id": f"r{nid:03d}", "class": cls, "verified": False,
                    "retriever": retriever, "assumptions": assumptions,
                    "input": inp, "expected": expected, "rationale": rationale})
        nid += 1

    for nonce in _fresh_nonces(rng, 30, taken):          # unique-term ×30
        docs = {}
        pos = rng.randrange(6)
        ds = rng.sample(DISTRACTORS, 5)
        j = 0
        for i in range(6):
            key = f"d{i+1}"
            if i == pos:
                docs[key] = (f"The {nonce} gauge settled at thirty units after "
                             f"the autumn service visit.")
                needle = key
            else:
                docs[key] = ds[j]; j += 1
        emit("unique_term_needle_bm25", "bm25",
             {"k": 5, "docs": docs, "query": f"{nonce} gauge"},
             {"rank_of": {needle: 1}},
             f"'{nonce}' occurs in exactly one document — must rank first.",
             ["query term appears in exactly one doc"])

    triples = list(zip(_COMMONS, _MIDWORDS, _RAREWORDS))
    for common, mid, rare in triples:                     # graded ×25
        docs = {
            "d1": f"The {rare} on the {mid} {common} was replaced early on Saturday morning.",
            "d2": f"A {mid} {common} stood by the doorway while the movers worked upstairs.",
            "d3": f"The {common} was cleaned and put away before the guests arrived tonight.",
            "d4": "The museum's new exhibit features maritime paintings from the previous century.",
        }
        emit("graded_overlap_order", "bm25",
             {"k": 4, "docs": docs, "query": f"{mid} {common} {rare}"},
             {"order": ["d1", "d2", "d3", "d4"]},
             "Overlap gradient 3>2>1>0, comparable lengths — BM25 order must be monotone.",
             ["comparable doc lengths"])

    for name, m in _PROCEDURES2:                          # composition ×25
        docs = {}
        gold_ids = []
        for i in range(m):
            key = f"g{i+1}"
            gold_ids.append(key)
            docs[key] = f"Step {i+1} of the {name} procedure: {_ACTIONS2[i % len(_ACTIONS2)]}."
        for i, d in enumerate(rng.sample(DISTRACTORS, 5), 1):
            docs[f"x{i}"] = d
        emit("composition_m_gt_k", "bm25",
             {"k": 5, "docs": docs, "gold_ids": gold_ids,
              "query": f"list the steps of the {name} procedure in order"},
             {"max_gold_in_topk": 5, "composition_bound_flag": True},
             f"m={m} > k=5 — the accounting must label this composition-bound.",
             ["composition_bound computed from gold multiplicity vs k"])

    for nonce in _fresh_nonces(rng, 30, taken):           # ghost ×30
        emit("post_eviction_ghost", "bm25",
             {"k": 5, "setup": [
                 {"op": "insert", "item": "n1",
                  "text": f"The {nonce} threshold constant is four point six for bay nine."},
                 {"op": "insert", "item": "n2",
                  "text": rng.choice(DISTRACTORS)},
                 {"op": "insert", "item": "n3",
                  "text": rng.choice(DISTRACTORS)},
                 {"op": "evict_scripted", "item": "n1"}],
              "query": f"{nonce} threshold constant"},
             {"results_containing": {"n1": "never"}},
             "Index-consistency invariant: evicted items are unreachable.",
             ["scripted_eviction available", "index updated on evict"])

    assert len(out) == 110, len(out)
    return out


# ══ verdict triplets (+120 deterministic) ═════════════════════════════

_FACTS = [
    ("April 9th at 3pm", "When is the dentist appointment?"),
    ("maple-syrup-42", "What is the cabin wifi password?"),
    ("14 Alder Grove, Leeds", "What is the sister's address?"),
    ("750 ml of vegetable stock", "How much stock does the soup need?"),
    ("Casa Verde", "Where is the anniversary dinner booked?"),
    ("June 2nd", "When is the library book due?"),
    ("30-22-14", "What is the gym locker combination?"),
    ("180 euros", "How much did the plumber quote?"),
    ("level 5, spot B12", "Where is the rental car parked?"),
    ("the coastal route", "Which route are they taking?"),
    ("platform 3", "Which platform does the train leave from?"),
    ("Bluebird", "What is the project codename?"),
    ("pier 7", "Which pier does the ferry leave from?"),
    ("Riverside Park", "Where does the under-12 team train?"),
    ("otter-lamp-9", "What is the shared drive password?"),
    ("8265", "What is the storage unit gate code?"),
    ("412", "What is Dr. Ferreira's room number?"),
    ("45 seconds", "After how long does the generator start?"),
    ("19 degrees", "What is the thermostat set to at night?"),
    ("233", "What is her locker number at the swim center?"),
    ("the Aurora room", "Which room is the workshop in?"),
    ("seat 12A", "Which seat is the user in?"),
    ("March 1st", "When is the deposit due?"),
    ("10 mg", "What is the prescribed dose?"),
    ("the Marlin Inn", "Where are they staying?"),
    ("6pm", "When does the evening class start?"),
    ("Biscuit", "What is the dog's name?"),
    ("Friday", "When is the essay deadline?"),
    ("Hall B", "Where is badge pickup?"),
    ("row 14", "Which row are the concert seats in?"),
]

_ABSTAIN_FORMS2 = ["IDK", "I don't know", "I do not know", "not enough information",
                   "insufficient information", "I cannot answer", "I can't answer",
                   "no relevant information"]

_UNANSWERABLE = [
    "Which hotel did the user book?", "What is the dentist's name?",
    "What color is the rental car?", "Who owns the cabin?",
    "What time is the reservation?", "What is the book's title?",
    "When will the plumber arrive?", "Which airline are they flying?",
    "What is the neighbor's surname?", "How long is the warranty?",
    "What size is the jacket?", "Which floor is the office on?",
    "What is the cat's name?", "When was the boiler last serviced?",
    "What brand is the lawnmower?", "Who recommended the restaurant?",
    "What is the meeting agenda?", "Which key opens the shed?",
    "How much was the parking fine?", "What is the boat called?",
    "Which month is the audit?", "What flavor was the cake?",
    "Who is picking up the kids?", "What is the gate agent's name?",
    "Which pharmacy is open late?", "What is the tracking number?",
    "When does the lease start?", "What is the coach's phone number?",
    "Which trail did they hike?", "What is the invoice number?",
]

_WRONG_ANSWERS = [
    "on May 22nd in the early morning", "sunflower-oat-77", "9 Birch Way, York",
    "two cups of chicken broth", "the Golden Fork downtown", "August 15th",
    "11-45-90", "320 euros", "level 2, spot A3", "the inland motorway",
    "gate 11", "Kestrel", "pier 12", "Elmwood Stadium", "badger-kite-4",
    "4197", "room 208", "ninety seconds", "25 degrees", "locker 508",
    "the Baltic room", "seat 30F", "September 30th", "50 mg",
    "the Harbor Lodge", "9:30pm", "Pretzel", "next Wednesday", "Hall D", "row 2",
]


def _exact_variant(rng: random.Random, ref: str) -> str:
    """A normalization-level variant: case shuffle, trailing punctuation,
    article add/drop — must stay normalized-equal."""
    a = ref
    choice = rng.randrange(4)
    if choice == 0:
        a = a.lower()
    elif choice == 1:
        a = a.upper()
    elif choice == 2:
        a = a + "."
    else:
        a = ("the " + a) if not a.lower().startswith("the ") else a[4:]
    return a


def gen_verdicts2(rng: random.Random) -> list[dict]:
    out: list[dict] = []
    nid = 61

    def emit(cls, inp, expected, tier, rationale):
        nonlocal nid
        out.append({"id": f"j{nid:03d}", "class": cls, "verified": False,
                    "input": inp, "expected": expected, "tier": tier,
                    "rationale": rationale})
        nid += 1

    for ref, q in _FACTS:                                   # exact ×30
        ans = _exact_variant(rng, ref)
        assert normalized_em(ans, ref), (ans, ref)
        emit("exact_match", {"reference": ref, "answer": ans, "question": q},
             "CORRECT", "deterministic", "Normalization-level equality.")
    for i, (ref, q) in enumerate(_FACTS):                   # abstain_wrong ×30
        emit("abstain_wrong",
             {"reference": ref, "answer": _ABSTAIN_FORMS2[i % len(_ABSTAIN_FORMS2)],
              "question": q},
             "INCORRECT", "deterministic", "Reference exists; abstention fails here.")
    for i, q in enumerate(_UNANSWERABLE):                   # abstain_correct ×30
        emit("abstain_correct",
             {"reference": None, "reference_type": "unanswerable",
              "answer": _ABSTAIN_FORMS2[(i + 3) % len(_ABSTAIN_FORMS2)], "question": q},
             "CORRECT", "deterministic", "Unanswerable — abstention is right.")
    for (ref, q), wrong in zip(_FACTS, _WRONG_ANSWERS):     # hallucination ×30
        ans = f"It was {wrong}, as noted earlier."
        assert not contains_match(ans, ref), (ans, ref)
        emit("hallucination", {"reference": ref, "answer": ans, "question": q},
             "INCORRECT", "deterministic_and_judge",
             "Fluent, confident, wrong entity.")

    assert len(out) == 120, len(out)
    return out


# ══ reader hard negatives (+15, hand-authored) ════════════════════════

_HARD_NEGATIVES = [
    (["User: Conference badge pickup is in Hall B; the welcome reception starts at 6pm on the terrace.",
      "User: Parking at the conference center costs 12 euros a day, validated at the front desk."],
     "What is the wifi password at the conference center?"),
    (["User: The landlord repainted the kitchen and replaced the dishwasher in March.",
      "User: Rent is due on the 1st and the building has bike storage in the basement."],
     "What is the apartment's square meterage?"),
    (["User: My flight lands in Lisbon at 11:40 and the taxi rank is right outside arrivals.",
      "User: I packed the blue suitcase and left a spare key with Rui."],
     "Which hotel am I staying at in Lisbon?"),
    (["User: The marathon route passes the old mill at kilometer 12 and the aid station is at 15.",
      "User: I've been averaging 5:40 per kilometer in training this month."],
     "What is my bib number for the marathon?"),
    (["User: The book club meets Thursdays at Nora's place; we're on chapter nine.",
      "User: Nora always makes the lemon cake everyone fights over."],
     "Which book is the book club reading?"),
    (["User: The garage replaced the timing belt and rotated the tires on Tuesday.",
      "User: The courtesy car they lent me smells of pine air freshener."],
     "How much did the garage charge for the service?"),
    (["User: My physiotherapy sessions are twice a week at the clinic on Harbor Street.",
      "User: The exercises focus on my left shoulder after the climbing injury."],
     "What is my physiotherapist's name?"),
    (["User: The wedding is outdoors at the botanical garden with a jazz trio for the ceremony.",
      "User: Dress code is garden formal and the forecast looks dry."],
     "What time does the wedding ceremony start?"),
    (["User: We planted two apple trees and a plum tree along the south fence last autumn.",
      "User: The plum flowered first this spring, weirdly."],
     "Which apple varieties did we plant?"),
    (["User: The server migration is scheduled for the weekend; downtime window is announced.",
      "User: Rollback plan is documented and the on-call rota is updated."],
     "How long is the downtime window for the migration?"),
    (["User: Grandpa's watch was serviced and the strap replaced with brown leather.",
      "User: The watchmaker said the movement is from the 1960s and worth insuring."],
     "How much is grandpa's watch insured for?"),
    (["User: The choir's winter concert is at the stone church with the good acoustics.",
      "User: We're singing two Bruckner motets and a Poulenc piece."],
     "On what date is the winter concert?"),
    (["User: The daycare sends photos every afternoon and does forest walks on Fridays.",
      "User: Pickup is any time before six; late fees are steep."],
     "What is the monthly daycare fee?"),
    (["User: The startup's runway update was shared in the all-hands; morale is decent.",
      "User: We're hiring two backend engineers this quarter."],
     "How many months of runway does the startup have?"),
    (["User: The ferry to the island runs hourly in summer and the crossing is 25 minutes.",
      "User: Dogs are allowed on the upper deck if leashed."],
     "How much does the ferry ticket cost?"),
]


def gen_hard_negatives() -> list[dict]:
    out = []
    for i, (evidence, question) in enumerate(_HARD_NEGATIVES):
        out.append({
            "id": f"a{101 + i:03d}",
            "class": "related_but_insufficient_abstain",
            "verified": False,
            "input": {"evidence": evidence, "question": question},
            "expected": {"must_abstain": True},
            "rationale": "Hard negative: the evidence is RICH in adjacent specifics "
                         "on the same topic but the asked fact is absent — the "
                         "D6-purge bluffing mode. Any answer here is fabrication.",
        })
    assert len(out) == 15
    return out


def main() -> None:
    e2 = gen_corpus_e2(random.Random(SEED))
    (CORP / "corpus_e" / "e_fixtures_bulk2.yaml").write_text(
        "# corpus_e sprint-2 growth — scripts/bulk_corpora_sprint2.py "
        f"(seed {SEED}); verified: false until author check.\n"
        + yaml.safe_dump(e2, sort_keys=False, width=100))
    r2 = gen_corpus_r2(random.Random(SEED + 1))
    (CORP / "corpus_r" / "r_fixtures_bulk2.yaml").write_text(
        "# corpus_r sprint-2 growth (deterministic classes).\n"
        + yaml.safe_dump(r2, sort_keys=False, width=100))
    j2 = gen_verdicts2(random.Random(SEED + 2))
    (CORP / "corpus_a_judge" / "verdict_triplets_bulk2.yaml").write_text(
        "# verdict_triplets sprint-2 growth (deterministic tiers).\n"
        + yaml.safe_dump(j2, sort_keys=False, width=100))
    hn = gen_hard_negatives()
    (CORP / "corpus_a_reader" / "reader_fixtures_bulk2.yaml").write_text(
        "# corpus_a_reader sprint-2: related_but_insufficient_abstain hard negatives.\n"
        + yaml.safe_dump(hn, sort_keys=False, width=100))
    for name, n in [("e_fixtures_bulk2", len(e2)), ("r_fixtures_bulk2", len(r2)),
                    ("verdict_triplets_bulk2", len(j2)), ("reader_fixtures_bulk2", len(hn))]:
        print(f"wrote {name}: {n}")


if __name__ == "__main__":
    main()
