"""Content pools + generators for the non-eviction corpora (see bulk_corpora.py).

All content is hand-authored here (surface-varied per the exemplar patterns);
the functions only assemble and number it. Every fixture ships verified: false.
"""
from __future__ import annotations

import random

# ── shared distractor pool (no procedure/step/list/order words; no nonces) ──
DISTRACTORS = [
    "The quarterly budget review covered travel costs and staffing plans for the region.",
    "Team offsite notes: venue booked, catering confirmed, agenda drafted for Thursday.",
    "Weather delayed the shipment; the port authority rescheduled unloading to Friday.",
    "Annual review templates were updated to include the new competency framework.",
    "Basketball practice moved to the east gym while the main court is resurfaced.",
    "The museum's new exhibit features maritime paintings from the previous century.",
    "Lunch options near the office now include a Vietnamese place on Elm Street.",
    "Ticket prices for the festival increase after the early-bird window closes.",
    "The garden needs mulching before the first frost arrives in late October.",
    "Her thesis defense was rescheduled because two committee members were traveling.",
]

# ══ corpus_r ═════════════════════════════════════════════════════════

_NONCES_BM25 = ["vorquastine", "blenthorpic", "drazzlefin", "morvexital",
                "quenzabrol", "xilthroped", "gravenolux", "pilcrovane", "thurmagant"]

_PARAPHRASE_PAIRS = [
    ("Her flight to Osaka departs at dawn on the twelfth.",
     "when does she leave for Japan?"),
    ("The physician recommended cutting back on salty snacks.",
     "what did the doctor advise about sodium intake?"),
    ("Their apartment lease ends in November.",
     "when must they move out of the flat?"),
    ("He bought a secondhand pickup with low mileage.",
     "what vehicle did he purchase?"),
    ("The orchestra rehearses every Thursday evening.",
     "when does the symphony group practice?"),
    ("She teaches beginners how to swim at the community pool.",
     "what sport does she instruct novices in?"),
    ("The bakery raised the price of its sourdough loaf.",
     "which shop made bread more expensive?"),
    ("His laptop battery drains within an hour.",
     "what is wrong with his computer's power?"),
    ("They postponed the wedding until next spring.",
     "when will the marriage ceremony happen?"),
]

# (common, mid, rare): d1 holds all three, d2 = common+mid, d3 = common, d4 none.
_OVERLAP_TRIPLES = [
    ("kettle", "green", "whistle"), ("ladder", "wooden", "rung"),
    ("locket", "silver", "clasp"), ("staircase", "marble", "banister"),
    ("spool", "copper", "sheath"), ("curtain", "velvet", "fringe"),
    ("wardrobe", "cedar", "hinge"), ("driveway", "gravel", "culvert"),
    ("awning", "canvas", "grommet"),
]

_PROCEDURES = [("kiln firing", 7), ("orchard grafting", 6), ("telescope alignment", 8),
               ("cider pressing", 6), ("darkroom developing", 7), ("beekeeping inspection", 8),
               ("kayak lamination", 6), ("cheese affinage", 7), ("loom threading", 8)]

_ACTIONS = ["prepare the workspace", "check the seals", "warm the chamber",
            "note the readings", "clean the fixtures", "confirm the levels",
            "secure the housing", "log the batch"]

_NONCES_GHOST = ["zenqorip", "fulvantrix", "obrestane", "kelvorium", "drapheme",
                 "wistolane", "cravonyx", "melforant", "surquilate"]


def gen_corpus_r(rng: random.Random) -> list[dict]:
    out = []
    nid = 6

    def emit(cls, retriever, inp, expected, rationale, assumptions):
        nonlocal nid
        out.append({"id": f"r{nid:03d}", "class": cls, "verified": False,
                    "retriever": retriever, "assumptions": assumptions,
                    "input": inp, "expected": expected, "rationale": rationale})
        nid += 1

    for nonce in _NONCES_BM25:
        docs = {}
        needle_pos = rng.randrange(6)
        ds = rng.sample(DISTRACTORS, 5)
        j = 0
        for i in range(6):
            key = f"d{i+1}"
            if i == needle_pos:
                docs[key] = (f"The {nonce} reading stabilised at forty units "
                             f"after the spring recalibration.")
                needle_key = key
            else:
                docs[key] = ds[j]; j += 1
        emit("unique_term_needle_bm25", "bm25",
             {"k": 5, "docs": docs, "query": f"{nonce} reading"},
             {"rank_of": {needle_key: 1}},
             f"'{nonce}' occurs in exactly one document; any lexical retriever must rank it "
             "first. A miss is an indexing/tokenization bug, not a relevance judgment.",
             ["query term appears in exactly one doc"])

    for doc, query in _PARAPHRASE_PAIRS:
        docs = {}
        needle_pos = rng.randrange(6)
        ds = rng.sample(DISTRACTORS, 5)
        j = 0
        for i in range(6):
            key = f"d{i+1}"
            if i == needle_pos:
                docs[key] = doc; needle_key = key
            else:
                docs[key] = ds[j]; j += 1
        emit("paraphrase_needle_dense", "dense",
             {"k": 5, "docs": docs, "query": query},
             {"rank_of": {needle_key: {"lte": 3}}},
             "Zero content-word overlap between query and needle; semantic match only. "
             "Distractors unrelated, so top-3 is a lenient, stable bar. BM25 is EXPECTED "
             "to fail this fixture — dense-only by design.",
             ["dense retriever is the configured embedder",
              "top-3 tolerance reflects embedder variance"])

    for common, mid, rare in _OVERLAP_TRIPLES:
        docs = {
            "d1": f"The {rare} on the {mid} {common} was replaced early on Saturday morning.",
            "d2": f"A {mid} {common} stood by the doorway while the movers worked upstairs.",
            "d3": f"The {common} was cleaned and put away before the guests arrived tonight.",
            "d4": "The museum's new exhibit features maritime paintings from the previous century.",
        }
        emit("graded_overlap_order", "bm25",
             {"k": 4, "docs": docs, "query": f"{mid} {common} {rare}"},
             {"order": ["d1", "d2", "d3", "d4"]},
             "Strict overlap gradient 3>2>1>0 with comparable doc lengths; BM25 order must "
             "be monotone in overlap regardless of exact idf values.",
             ["docs of comparable length so term overlap dominates length normalization"])

    for name, m in _PROCEDURES:
        docs = {}
        gold_ids = []
        for i in range(m):
            key = f"g{i+1}"
            gold_ids.append(key)
            docs[key] = f"Step {i+1} of the {name} procedure: {_ACTIONS[i % len(_ACTIONS)]}."
        for i, d in enumerate(rng.sample(DISTRACTORS, 5), 1):
            docs[f"x{i}"] = d
        emit("composition_m_gt_k", "bm25",
             {"k": 5, "docs": docs, "gold_ids": gold_ids,
              "query": f"list the steps of the {name} procedure in order"},
             {"max_gold_in_topk": 5, "composition_bound_flag": True},
             f"m={m} gold cannot fit in top-5 — the arithmetic ceiling. The TEST is the "
             "accounting: the harness must label this query composition-bound rather than "
             "scoring the answer stage as a policy failure.",
             ["composition_bound computed from gold multiplicity vs k"])

    for nonce in _NONCES_GHOST:
        emit("post_eviction_ghost", "bm25",
             {"k": 5, "setup": [
                 {"op": "insert", "item": "n1",
                  "text": f"The {nonce} calibration constant is nine point two for unit five."},
                 {"op": "insert", "item": "n2",
                  "text": "Cafeteria menu rotates weekly; Thursday is soup day."},
                 {"op": "insert", "item": "n3",
                  "text": rng.choice(DISTRACTORS)},
                 {"op": "evict_scripted", "item": "n1"}],
              "query": f"{nonce} calibration constant"},
             {"results_containing": {"n1": "never"}},
             "Index-consistency invariant (S5): an evicted item must be unreachable by any "
             "subsequent retrieval. A ghost hit silently corrupts every reach number.",
             ["scripted_eviction available", "index updated on evict"])

    assert len(out) == 45, len(out)
    return out


# ══ corpus_a_reader ══════════════════════════════════════════════════

_VERBATIM = [
    ("User: My dentist appointment is on April 9th at 3pm.",
     "When is the user's dentist appointment?", ["April 9"]),
    ("User: The wifi password at the cabin is 'maple-syrup-42'.",
     "What is the wifi password at the cabin?", ["maple-syrup-42"]),
    ("User: I parked the rental car on level 5, spot B12.",
     "Where is the rental car parked?", ["B12", "level 5"]),
    ("User: My sister's new address is 14 Alder Grove, Leeds.",
     "What is the user's sister's address?", ["14 Alder Grove"]),
    ("User: The soup recipe needs 750 ml of vegetable stock.",
     "How much stock does the soup recipe need?", ["750"]),
    ("User: Our anniversary dinner is booked at Casa Verde.",
     "Where is the anniversary dinner booked?", ["Casa Verde"]),
    ("User: The library book is due back on June 2nd.",
     "When is the library book due?", ["June 2"]),
    ("User: My gym locker combination is 30-22-14.",
     "What is the gym locker combination?", ["30-22-14"]),
    ("User: The plumber quoted 180 euros for the repair.",
     "How much did the plumber quote?", ["180"]),
]

_ABSENT = [
    ("User: I finally booked the flights. We land in Lisbon on May 14th.",
     "Which airline is the user flying?"),
    ("User: My dentist appointment is on April 9th at 3pm.",
     "What is the dentist's name?"),
    ("User: The soup recipe needs 750 ml of vegetable stock.",
     "How long should the soup simmer?"),
    ("User: Our anniversary dinner is booked at Casa Verde.",
     "What time is the dinner reservation?"),
    ("User: I parked the rental car on level 5, spot B12.",
     "What color is the rental car?"),
    ("User: The plumber quoted 180 euros for the repair.",
     "When will the plumber arrive?"),
    ("User: My sister's new address is 14 Alder Grove, Leeds.",
     "What is the sister's phone number?"),
    ("User: The library book is due back on June 2nd.",
     "What is the title of the library book?"),
    ("User: The wifi password at the cabin is 'maple-syrup-42'.",
     "Who owns the cabin?"),
]

_DERIVATION = [
    (["User: The seedlings were planted on June 2nd.",
      "User: The first sprouts appeared on June 11th."],
     "How many days passed between planting and the first sprouts?", ["9", "nine"]),
    (["User: The rent was 900 euros a month last year.",
      "User: The landlord raised the rent by 75 euros."],
     "What is the monthly rent now?", ["975"]),
    (["User: She bought 3 notebooks on Monday.",
      "User: She bought 4 more notebooks on Friday."],
     "How many notebooks did she buy in total?", ["7", "seven"]),
    (["User: The movie starts at 8pm.", "User: The movie runs for exactly 2 hours."],
     "When does the movie end?", ["10"]),
    (["User: The rain tank holds 60 liters when full.",
      "User: The tank is currently exactly half full."],
     "How many liters are in the tank right now?", ["30", "thirty"]),
    (["User: Tickets cost 25 euros each.", "User: We bought 4 tickets."],
     "How much did the tickets cost in total?", ["100"]),
    (["User: The trailhead sits at 400 meters elevation.",
      "User: The hike climbs another 850 meters to the summit."],
     "What is the elevation at the summit?", ["1250", "1,250"]),
    (["User: His shift starts on Tuesday.", "User: The shift lasts exactly 3 days."],
     "On which day does the shift end?", ["Thursday"]),
    (["User: The recipe serves four and calls for 2 eggs.",
      "User: We are doubling the recipe."],
     "How many eggs will we need?", ["4", "four"]),
]

_ENUMERATION = [
    ("User: The backup procedure is: first export the database, second compress the "
     "archive, third upload it to the vault, fourth verify the checksum.",
     "List the steps of the backup procedure in order.",
     ["export", "compress", "upload", "verify"]),
    ("User: The bread method is: first knead the dough, second proof it overnight, "
     "third shape the loaf, fourth bake at high heat.",
     "List the steps of the bread method in order.",
     ["knead", "proof", "shape", "bake"]),
    ("User: Onboarding goes: first collect your badge, second pick up the laptop, "
     "third attend orientation, fourth meet your mentor.",
     "List the onboarding steps in order.",
     ["badge", "laptop", "orientation", "mentor"]),
    ("User: Camp setup is: first pitch the tent, second stake the guylines, third "
     "inflate the sleeping pads, fourth stow the food barrel.",
     "List the camp setup steps in order.",
     ["pitch", "stake", "inflate", "stow"]),
    ("User: Bike maintenance is: first degrease the chain, second lubricate it, "
     "third adjust the brakes, fourth pump the tires.",
     "List the bike maintenance steps in order.",
     ["degrease", "lubricate", "brakes", "tires"]),
    ("User: The release drill is: first freeze the branch, second migrate the "
     "schema, third restart the workers, fourth announce in the channel.",
     "List the release drill steps in order.",
     ["freeze", "migrate", "restart", "announce"]),
    ("User: The lab protocol is: first calibrate the probe, second dilute the "
     "sample, third incubate for an hour, fourth measure the absorbance.",
     "List the lab protocol steps in order.",
     ["calibrate", "dilute", "incubate", "measure"]),
    ("User: Autumn garden care: first weed the beds, second mulch them, third "
     "prune the roses, fourth water everything deeply.",
     "List the autumn garden steps in order.",
     ["weed", "mulch", "prune", "water"]),
    ("User: Moving day plan: first pack the boxes, second label every box, third "
     "load the van, fourth unload at the new place.",
     "List the moving day steps in order.",
     ["pack", "label", "load", "unload"]),
]

_HEDGED = [
    ("User: We'll probably take the coastal route — the mountain pass might still be icy.",
     "Which route are they leaning toward?", ["coastal"]),
    ("User: I think the meeting moved to Thursday, though IDK if the invite updated.",
     "When is the meeting likely to be?", ["Thursday"]),
    ("User: She'll most likely pick the aisle seat; not sure about the row.",
     "Which seat is she leaning toward?", ["aisle"]),
    ("User: The landlord will presumably renew the lease, but nothing is signed yet.",
     "What does the landlord seem likely to do?", ["renew"]),
    ("User: I can't say for certain, but the venue is almost surely the town hall.",
     "Which venue is most likely?", ["town hall"]),
    ("User: We're leaning toward the hybrid model, though the specs aren't final.",
     "Which model are they leaning toward?", ["hybrid"]),
    ("User: He probably left the keys with the doorman — that's his habit.",
     "Where are the keys most likely to be?", ["doorman"]),
    ("User: My guess is the shipment lands Friday; the tracking page is vague.",
     "When does the shipment probably arrive?", ["Friday"]),
    ("User: The vendor hasn't confirmed, but the deadline looks like October.",
     "When is the deadline expected to be?", ["October"]),
]

_POSITION_NEEDLES = [
    ("User: The storage unit gate code is 8265.", "What is the storage unit gate code?", ["8265"]),
    ("User: The project codename was changed to Bluebird.", "What is the project codename now?", ["Bluebird"]),
    ("User: Dr. Ferreira's clinic room number is 412.", "What is Dr. Ferreira's room number?", ["412"]),
    ("User: The backup generator kicks in after 45 seconds.", "After how many seconds does the backup generator start?", ["45"]),
    ("User: The charity auction raised 5840 euros.", "How much did the charity auction raise?", ["5840"]),
    ("User: The password for the shared drive is 'otter-lamp-9'.", "What is the shared drive password?", ["otter-lamp-9"]),
    ("User: The ferry to the island leaves from pier 7.", "Which pier does the island ferry leave from?", ["7"]),
    ("User: The thermostat is set to 19 degrees at night.", "What is the thermostat set to at night?", ["19"]),
    ("User: Her locker at the swim center is number 233.", "What is her locker number at the swim center?", ["233"]),
]

_SYNTHESIS = [
    (["User: Alice moved to Lisbon in January.", "User: Alice adopted a kitten called Miso."],
     "In which city does the kitten Miso live?", ["Lisbon"]),
    (["User: The red suitcase belongs to Marta.", "User: Marta is flying to Oslo tomorrow."],
     "Where is the red suitcase headed?", ["Oslo"]),
    (["User: Ben's allotment grows only garlic.", "User: The festival stall sells whatever Ben grows."],
     "What does the festival stall sell?", ["garlic"]),
    (["User: The blue folder holds the tax documents.", "User: I left the blue folder at grandma's house."],
     "Where are the tax documents?", ["grandma"]),
    (["User: Priya coaches the under-12 football team.", "User: The under-12 team trains at Riverside Park."],
     "Where does Priya coach?", ["Riverside"]),
    (["User: The antique clock was inherited from uncle Tomas.", "User: The antique clock now sits in the study."],
     "Where is uncle Tomas's clock now?", ["study"]),
    (["User: Nadia's thesis is about coral reefs.", "User: Nadia will present her thesis in Cairns."],
     "Where will the coral reef research be presented?", ["Cairns"]),
    (["User: The bakery van breaks down every Monday.", "User: When the van is down, deliveries go by bicycle."],
     "How are bakery deliveries made on Mondays?", ["bicycle"]),
    (["User: Tom lent his drill to Sara.", "User: Sara is renovating the attic."],
     "Which project is Tom's drill being used for?", ["attic"]),
    (["User: The gallery's east wing shows only photography.", "User: Lena's exhibition is in the east wing."],
     "What kind of art is in Lena's exhibition?", ["photograph"]),
]

_CONFLICT = [
    (["User: The invoice total was 420 euros.", "User: The invoice total was 470 euros."],
     "What was the invoice total?", ["420", "470"]),
    (["User: The workshop is in the Aurora room.", "User: The workshop is in the Baltic room."],
     "Which room is the workshop in?", ["Aurora", "Baltic"]),
    (["User: My seat on the flight is 12A.", "User: My seat on the flight is 14C."],
     "Which seat is the user in?", ["12A", "14C"]),
    (["User: The deposit is due on March 1st.", "User: The deposit is due on March 8th."],
     "When is the deposit due?", ["March 1", "March 8"]),
    (["User: The prescribed dose is 10 mg.", "User: The prescribed dose is 20 mg."],
     "What is the prescribed dose?", ["10", "20"]),
    (["User: We're staying at the Marlin Inn.", "User: We're staying at the Harbor Lodge."],
     "Where are they staying?", ["Marlin", "Harbor"]),
    (["User: The evening class starts at 6pm.", "User: The evening class starts at 7pm."],
     "When does the evening class start?", ["6", "7"]),
    (["User: The dog is called Biscuit.", "User: The dog is called Waffles."],
     "What is the dog's name?", ["Biscuit", "Waffles"]),
    (["User: The train leaves from platform 3.", "User: The train leaves from platform 9."],
     "Which platform does the train leave from?", ["3", "9"]),
    (["User: The essay deadline is Friday.", "User: The essay deadline is Monday."],
     "When is the essay deadline?", ["Friday", "Monday"]),
]


def gen_corpus_a(rng: random.Random) -> list[dict]:
    out = []
    nid = 9

    def emit(cls, inp, expected, rationale):
        nonlocal nid
        out.append({"id": f"a{nid:03d}", "class": cls, "verified": False,
                    "input": inp, "expected": expected, "rationale": rationale})
        nid += 1

    for ev, q, must in _VERBATIM:
        emit("verbatim_present_answer", {"evidence": [ev], "question": q},
             {"must_answer": True, "answer_must_contain_any": must},
             "Answer verbatim in a single short evidence item; failure = prompt or "
             "post-processing bug, never a knowledge issue.")
    for ev, q in _ABSENT:
        emit("absent_abstain", {"evidence": [ev], "question": q},
             {"must_abstain": True},
             "Related topic, absent fact — answering here is hallucination.")
    for evs, q, must in _DERIVATION:
        emit("derivation_compute", {"evidence": evs, "question": q},
             {"must_answer": True, "answer_must_contain_any": must},
             "All needed facts present; answer requires one arithmetic/calendar step. "
             "A derivation-forbidding prompt fails here BY DESIGN (a003 class).")
    for ev, q, must in _ENUMERATION:
        emit("enumeration_full_list", {"evidence": [ev], "question": q},
             {"must_answer": True, "answer_must_contain_all": must},
             "All four steps must survive generation AND post-processing (first-line "
             "mutilation / generation-cap detectors).")
    for ev, q, must in _HEDGED:
        emit("hedged_not_abstention", {"evidence": [ev], "question": q},
             {"must_answer": True, "answer_must_contain_any": must,
              "must_not_be_flagged_abstention": True},
             "A correct answer that legitimately hedges — guards the abstention matcher "
             "against substring false positives (a005 class).")
    for needle, q, must in _POSITION_NEEDLES:
        for pos in ("head", "mid", "tail"):
            emit(f"position_{pos}",
                 {"filler_tokens": 3000, "needle": needle, "needle_position": pos,
                  "question": q},
                 {"must_answer": True, "answer_must_contain_any": must},
                 "Needle-in-haystack triplet on the 16k tag; head-only failure = "
                 "front truncation, tail-only = back truncation, mid degradation = "
                 "position bias (reported, not hidden). Same expected for all three — "
                 "divergence IS the measurement (a006–a008 class).")
    for evs, q, must in _SYNTHESIS:
        emit("multi_item_synthesis", {"evidence": evs, "question": q},
             {"must_answer": True, "answer_must_contain_any": must},
             "Two evidence items must be JOINED (entity bridging); either alone is "
             "insufficient. Tests cross-memory composition, not lookup.")
    for evs, q, must in _CONFLICT:
        emit("conflicting_evidence_flag", {"evidence": evs, "question": q},
             {"must_answer": True, "answer_must_contain_all": must},
             "Flat contradiction with no update cue: the reader must surface BOTH "
             "values (silently picking one is an unmarked coin-flip).")

    assert len(out) == 92, len(out)
    return out


# ══ verdict_triplets ═════════════════════════════════════════════════

_ABSTAIN_FORMS = ["IDK", "I don't know", "I do not know", "not enough information",
                  "insufficient information", "I cannot answer", "IDK."]

_J_EXACT = [
    ("June 2nd", "june 2nd", "When is the library book due?"),
    ("the blue vendor", "Blue vendor", "Which vendor is the user leaning toward?"),
    ("Casa Verde", "casa verde.", "Where is the anniversary dinner booked?"),
    ("pier 7", "Pier 7", "Which pier does the ferry leave from?"),
    ("Riverside Park", "riverside park", "Where does the team train?"),
    ("975 euros", "975 euros", "What is the rent now?"),
]

_J_PARAPHRASE = [
    ("He works as a cardiologist", "His job is treating heart conditions.", "What does he do?"),
    ("The flight departs at dawn", "The plane leaves very early in the morning.", "When does the flight leave?"),
    ("She lives in a rented flat", "Her home is a leased apartment.", "Where does she live?"),
    ("The bridge is closed for repairs", "Maintenance work has shut the crossing.", "Why can't we cross the bridge?"),
    ("He forgot his umbrella", "He left his rain cover behind.", "What did he forget?"),
    ("The store opens at noon", "The shop begins trading at midday.", "When does the store open?"),
    ("They won the final match", "The team took the championship game.", "How did the final go?"),
]

_J_PARTIAL = [
    ("export the database; compress the archive; upload to the vault; verify the checksum",
     "Export the database, then compress the archive.", "List the backup steps."),
    ("knead the dough; proof overnight; shape the loaf; bake at high heat",
     "Knead the dough.", "List the bread steps."),
    ("weed the beds; mulch them; prune the roses; water deeply",
     "Weed the beds and mulch them.", "List the autumn garden steps."),
    ("calibrate the probe; dilute the sample; incubate an hour; measure absorbance",
     "Calibrate the probe, then dilute the sample.", "List the lab protocol steps."),
    ("pack the boxes; label every box; load the van; unload at the new place",
     "Pack the boxes.", "List the moving steps."),
    ("pitch the tent; stake the guylines; inflate the pads; stow the food barrel",
     "Pitch the tent and stake the guylines.", "List the camp setup steps."),
]

_J_HALLUCINATION = [
    ("Casa Verde", "The dinner is booked at the Golden Fork downtown.", "Where is the dinner booked?"),
    ("April 9th at 3pm", "The appointment is on May 2nd in the morning.", "When is the appointment?"),
    ("level 5, spot B12", "The car is parked on level 2, spot A3.", "Where is the car parked?"),
    ("750 ml of vegetable stock", "You need two cups of chicken broth.", "How much stock is needed?"),
    ("the coastal route", "They decided on the inland motorway.", "Which route are they taking?"),
    ("platform 3", "The train departs from gate 11.", "Where does the train leave from?"),
    ("Biscuit", "The dog is called Pretzel.", "What is the dog's name?"),
]

_J_NUMERIC = [
    ("7 notebooks", "seven notebooks in total", "How many notebooks did she buy?"),
    ("30 liters", "thirty liters remain in the tank", "How many liters are in the tank?"),
    ("100 euros", "one hundred euros altogether", "How much did the tickets cost?"),
    ("45 seconds", "forty five seconds", "After how long does the generator start?"),
    ("19 degrees", "nineteen degrees", "What is the thermostat set to?"),
    ("9 days", "nine days passed", "How many days passed?"),
    ("1250 meters", "one thousand two hundred fifty meters", "What is the summit elevation?"),
]

_J_SUPERSET = [
    ("May 14th", "May 14th — and they're staying at the Hilton near the waterfront.",
     "When does the user arrive in Lisbon?"),
    ("Casa Verde", "Casa Verde, which apparently has a lovely rooftop terrace.",
     "Where is the dinner booked?"),
    ("platform 3", "Platform 3, right next to the coffee kiosk they always use.",
     "Which platform does the train leave from?"),
    ("the aisle seat", "The aisle seat — she also upgraded to extra legroom.",
     "Which seat did she pick?"),
    ("Bluebird", "Bluebird, chosen after a long naming meeting last week.",
     "What is the project codename?"),
    ("June 2nd", "June 2nd, and there's a small late fee if it slips.",
     "When is the book due?"),
]


def gen_verdicts(rng: random.Random) -> list[dict]:
    out = []
    nid = 9

    def emit(cls, inp, expected, tier, rationale, rubric_note=None):
        nonlocal nid
        fx = {"id": f"j{nid:03d}", "class": cls, "verified": False,
              "input": inp, "expected": expected, "tier": tier,
              "rationale": rationale}
        if rubric_note:
            fx["rubric_note"] = rubric_note
        out.append(fx)
        nid += 1

    for ref, ans, q in _J_EXACT:
        emit("exact_match", {"reference": ref, "answer": ans, "question": q},
             "CORRECT", "deterministic",
             "Normalization-level equality; deterministic EM must catch it without the judge.")
    for ref, ans, q in _J_PARAPHRASE:
        emit("paraphrase", {"reference": ref, "answer": ans, "question": q},
             "CORRECT", "judge_or_embedding",
             "Semantic equivalence with near-zero lexical overlap — the bracket's reason to exist.")
    for ref, ans, q in _J_PARTIAL:
        emit("partial_list", {"reference": ref, "answer": ans, "question": q},
             "INCORRECT", "judge",
             "Under half a procedure is not the procedure.",
             rubric_note="Rubric v1: list types need >=75% key-step coverage for CORRECT. "
                         "RUBRIC-SENSITIVE — expected changes WITH the rubric, same commit.")
    for i in range(6):
        ref, _, q = _J_EXACT[i]
        emit("abstain_wrong",
             {"reference": ref, "answer": _ABSTAIN_FORMS[i % len(_ABSTAIN_FORMS)], "question": q},
             "INCORRECT", "deterministic",
             "Reference exists; abstention is a failure HERE (sign test pair).")
    for i in range(7):
        emit("abstain_correct",
             {"reference": None, "reference_type": "unanswerable",
              "answer": _ABSTAIN_FORMS[i % len(_ABSTAIN_FORMS)],
              "question": ["Which hotel did the user book?", "What is the dentist's name?",
                           "What color is the rental car?", "Who owns the cabin?",
                           "What time is the reservation?", "What is the book's title?",
                           "When will the plumber arrive?"][i]},
             "CORRECT", "deterministic",
             "Unanswerable type; abstention is the right action (the 29/30 regression class).")
    for ref, ans, q in _J_HALLUCINATION:
        emit("hallucination", {"reference": ref, "answer": ans, "question": q},
             "INCORRECT", "deterministic_and_judge",
             "Fluent, confident, wrong — the judge must not be swayed by fluency.")
    for ref, ans, q in _J_NUMERIC:
        emit("numeric_format", {"reference": ref, "answer": ans, "question": q},
             "CORRECT", "judge_or_alias",
             "Format variance on a correct value; deterministic EM fails by design.")
    for ref, ans, q in _J_SUPERSET:
        emit("superset_answer", {"reference": ref, "answer": ans, "question": q},
             "CORRECT", "judge",
             "Covered reference plus unsupported garnish — record the garnish, don't fail it.",
             rubric_note="Rubric v1: unsupported EXTRA content does not flip a covered "
                         "reference to INCORRECT unless it contradicts it. RUBRIC-SENSITIVE.")

    assert len(out) == 52, len(out)
    return out
