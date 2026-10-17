"""Tiny saved-metadata fixtures; no network or production data."""
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from analysis.kaushik_kalshi_replication import sports_binding as s


def event(event_id="NATIVE-1", title="Alpha at Beta", series="KXNFLGAME"):
    return {"event_ticker": event_id, "series_ticker": series, "title": title, "sub_title": "opaque labels"}


def milestone(event_id="NATIVE-1", milestone_id="M1", title="Beta vs Alpha", **updates):
    row = {"id": milestone_id, "primary_event_tickers": [event_id], "title": title,
        "details": {"away_team_id": "native-A", "home_team_id": "native-B", "league": "NFL"},
        "start_date": "2026-01-02T01:00:00Z", "end_date": "2099-01-01T00:00:00Z"}
    row.update(updates)
    return row


def provider(game_id="P1", names=("Alpha", "Beta"), **updates):
    row = {"sport": "nfl", "provider_game_id": game_id, "participant_names": list(names),
        "scheduled_start_utc": "2026-01-02T01:00:00Z", "actual_start_utc": "2026-01-02T01:10:00Z",
        "actual_end_utc": "2026-01-02T04:00:00Z", "market_date": "2026-01-01",
        "timing_qualification": "provider_live_play_boundaries", "evidence": "pinned fixture identity/timing proof"}
    row.update(updates)
    return row


def alias(left, right, **updates):
    row = {"sport": "nfl", "native_name": left, "provider_name": right, "reviewed": True, "evidence": "independent fixture review"}
    row.update(updates)
    return row


class SportsBindingTests(unittest.TestCase):
    def native(self, events=None, milestones=None):
        return s.native_candidates([event()] if events is None else events, [milestone()] if milestones is None else milestones)

    def bind(self, native=None, providers=None, **options):
        return s.bind_candidates(self.native() if native is None else native,
            s.provider_games([provider()] if providers is None else providers), **options)

    def test_unique_unordered_pair_ignores_milestone_title_order_and_end_date(self):
        result = self.bind()[0]
        self.assertEqual(result["provider_game_id"], "P1")
        self.assertEqual(result["orientation"], "unknown")
        self.assertEqual(result["actual_end_utc"], "2026-01-02T04:00:00+00:00")
        self.assertFalse(result["sports_admitted"])
        self.assertEqual(result["native_yes_proposition_and_result_proof"], "pending")

    def test_orientation_conflict_is_only_an_unordered_identity_candidate(self):
        result = self.bind(providers=[provider(away_name="Alpha", home_name="Beta")])[0]
        self.assertEqual(result["orientation"], "verified_away_home")
        result = self.bind(providers=[provider(away_name="Beta", home_name="Alpha")])[0]
        self.assertIsNone(result["reason"])
        self.assertEqual(result["provider_game_id"], "P1")
        self.assertTrue(result["orientation_conflict"])
        self.assertFalse(result["home_away_proposition_admissible"])
        self.assertFalse(result["sports_admitted"])
        self.assertEqual(result["native_orientation"]["away_name"],"Alpha")
        self.assertEqual(result["provider_orientation"]["away_name"],"Beta")

    def test_exact_utc_date_not_market_date_or_nearest(self):
        result = self.bind(providers=[provider(scheduled_start_utc="2026-01-01T23:59:59Z")])[0]
        self.assertEqual(result["reason"], "no_exact_provider_pair_date")
        result = self.bind(providers=[provider(scheduled_start_utc=None)])[0]
        self.assertEqual(result["date_source"], "actual_start_utc")

    def test_missing_ambiguous_milestones_and_clock_have_explicit_reasons(self):
        self.assertEqual(self.native(milestones=[])[0]["reason"], "missing_primary_milestone")
        self.assertEqual(self.native(milestones=[milestone(), milestone(milestone_id="M2")])[0]["reason"], "ambiguous_primary_milestone")
        self.assertEqual(self.native(milestones=[milestone(start_date=None)])[0]["reason"], "missing_or_invalid_scheduled_start")
        self.assertEqual(self.native(milestones=[milestone(start_date="2026-01-02T01:00:00+01:00")])[0]["reason"], "missing_or_invalid_scheduled_start")
        duplicate = milestone(primary_event_tickers=["NATIVE-1","OTHER-HALF","OTHER-HALF"])
        self.assertEqual(self.native(milestones=[duplicate])[0]["reason"], "duplicate_milestone_primary_link")
        self.assertEqual(self.native(milestones=[duplicate])[0]["repeated_primary_links"], ["OTHER-HALF"])

    def test_event_and_milestone_participants_must_reconcile(self):
        native = self.native(events=[event(title="Gamma at Beta")])
        self.assertEqual(self.bind(native=native)[0]["reason"], "native_event_milestone_names_disagree")

    def test_aliases_require_explicit_review_and_pair_bijection(self):
        providers = [provider(names=("Alpha Wolves", "Beta Bears"))]
        self.assertEqual(self.bind(providers=providers)[0]["reason"], "no_exact_provider_pair_date")
        aliases = [alias("Alpha", "Alpha Wolves"), alias("Beta", "Beta Bears")]
        self.assertEqual(self.bind(providers=providers, reviewed_aliases=aliases)[0]["provider_game_id"], "P1")
        with self.assertRaisesRegex(s.BindingBlocked, "independent review"):
            self.bind(reviewed_aliases=[alias("Alpha", "Alpha", reviewed=False)])
        with self.assertRaisesRegex(s.BindingBlocked, "conflicting"):
            self.bind(reviewed_aliases=[alias("Alpha", "Alpha"), alias("Alpha", "Gamma")])
        self.assertEqual(self.bind(reviewed_aliases=[alias("Alpha", "Beta")])[0]["reason"], "nonbijective_native_alias_pair")

    def test_doubleheaders_and_duplicate_native_pair_dates_never_pick_one(self):
        result = self.bind(providers=[provider(), provider(game_id="P2", scheduled_start_utc="2026-01-02T07:00:00Z")])[0]
        self.assertEqual(result["reason"], "ambiguous_provider_pair_date_doubleheader")
        native = self.native(events=[event(), event("NATIVE-2")], milestones=[milestone(), milestone("NATIVE-2", "M2")])
        self.assertEqual({row["reason"] for row in self.bind(native=native)}, {"ambiguous_native_pair_date"})

    def test_global_identity_duplicates_and_sport_mismatch_block_or_exclude(self):
        with self.assertRaisesRegex(s.BindingBlocked, "duplicate native event"):
            self.native(events=[event(), event()])
        with self.assertRaisesRegex(s.BindingBlocked, "duplicate native milestone"):
            self.native(milestones=[milestone(), milestone()])
        with self.assertRaisesRegex(s.BindingBlocked, "duplicate provider game"):
            s.provider_games([provider(), provider()])
        wrong = milestone(details={"away_team_id": "A", "home_team_id": "B", "league": "NBA"})
        self.assertEqual(self.native(milestones=[wrong])[0]["reason"], "milestone_sport_disagreement")

    def test_nine_exact_series_only_no_mixed_tournament_expansion(self):
        self.assertEqual(len(s.WINNER_SERIES), 9)
        self.assertEqual(self.native(events=[event(series="KXATP")]), [])

    def test_date_outliers_and_invalid_actual_clocks_are_not_matched(self):
        games = s.provider_games([provider(market_date="2025-12-30")])
        self.assertEqual(games[0]["reason"], "provider_market_date_outlier_pending_identity_review")
        self.assertIsNone(s.bind_candidates(self.native(), games)[0]["provider_game_id"])
        self.assertEqual(s.provider_games([provider(actual_end_utc=None)])[0]["reason"], "invalid_provider_play_clocks")

    def test_atp_qualification_preserved_and_wrong_qualification_blocks(self):
        row = provider(sport="atp", timing_qualification="scheduled_start_plus_archived_duration")
        self.assertEqual(s.provider_games([row])[0]["timing_qualification"], "scheduled_start_plus_archived_duration")
        with self.assertRaisesRegex(s.BindingBlocked, "ATP timing qualification"):
            s.provider_games([provider(sport="atp")])

    def test_alias_proposals_require_cross_game_and_opponent_consistency_but_are_never_approved(self):
        events = [event("N1", "Alpha at Beta"), event("N2", "Alpha at Gamma")]
        milestones = [milestone("N1", "M1", "Alpha vs Beta"), milestone("N2", "M2", "Alpha vs Gamma", start_date="2026-01-03T01:00:00Z")]
        native = self.native(events=events, milestones=milestones)
        games = s.provider_games([provider(names=("Alpha Wolves", "Beta Bears")), provider("P2", names=("Alpha Wolves", "Gamma Greens"),
            scheduled_start_utc="2026-01-03T01:00:00Z", actual_start_utc="2026-01-03T01:10:00Z",actual_end_utc="2026-01-03T04:00:00Z",market_date="2026-01-03")])
        proposals = s.propose_aliases(native, games)
        alpha = next(row for row in proposals if row["native_name"] == "Alpha")
        self.assertTrue(alpha["cross_game_consistent"])
        self.assertFalse(alpha["reviewed"])
        self.assertEqual(s.bind_candidates(native, games)[0]["reason"], "no_exact_provider_pair_date")
        with self.assertRaisesRegex(s.BindingBlocked, "independent review"):
            s.bind_candidates(native, games, reviewed_aliases=proposals)

    def test_ambiguous_name_seeds_do_not_generate_aliases(self):
        games = s.provider_games([provider(names=("Alpha Wolves", "Beta Bears")), provider("P2", names=("Alpha Tigers", "Beta Bears"))])
        self.assertEqual(s.propose_aliases(self.native(), games), [])
        one_shared_opponent = s.provider_games([provider(names=("Gamma Greens", "Beta Bears"))])
        self.assertEqual(s.propose_aliases(self.native(), one_shared_opponent), [])

    def test_summary_is_explicitly_pending_and_includes_all_nine_sports(self):
        native, games = self.native(), s.provider_games([provider()])
        summary = s.review_summary(native, games, s.bind_candidates(native, games))
        self.assertEqual(summary["status"], "prototype_not_accepted")
        self.assertFalse(summary["sports_results_produced"])
        self.assertFalse(summary["native_end_date_used_as_actual_clock"])
        self.assertEqual(set(summary["by_sport"]), set(s.WINNER_SERIES.values()))
        self.assertEqual(summary["by_sport"]["nfl"]["identity_candidates"], 1)

    def test_native_entities_supply_exact_uuid_names_without_title_order_guess(self):
        entities = s.entity_identities([
            {"id":"native-A","name":"ALP Wolves","type":"football_team","details":{"league":"NFL","market":"Alpha","team_name":"ALP Wolves"}},
            {"id":"native-B","name":"Beta Bears","type":"football_team","details":{"league":"NFL","market":"Beta","team_name":"Beta Bears"}}], evidence="fixture pinned target reference")
        native = s.native_candidates([event()], [milestone()], entities=entities)
        self.assertEqual(native[0]["participant_names"], ["ALP Wolves","Beta Bears"])
        # A literal structured field may match; no guessed city+nickname join.
        result = self.bind(native=native, providers=[provider(names=("Alpha","Beta Bears"))])[0]
        self.assertEqual(result["provider_game_id"], "P1")
        self.assertEqual(result["title_label_review"], "pending_title_alias_review")
        self.assertEqual(result["native_participant_name_source"], "native_UUID_join_pinned_structured_targets")
        self.assertEqual(self.bind(native=native, providers=[provider(names=("Alpha Wolves","Beta Bears"))])[0]["reason"], "no_exact_provider_pair_date")
        self.assertEqual(self.native(events=[event("GDP-232022 Q4", series="KXGDP")]), [])

    def test_entity_ids_and_sports_are_checked_and_missing_mapping_is_explicit(self):
        row = {"id":"native-A","name":"Alpha","type":"football_team","details":{"league":"NFL"}}
        with self.assertRaisesRegex(s.BindingBlocked, "duplicate native entity"):
            s.entity_identities([row,row], evidence="fixture")
        entities = s.entity_identities([row], evidence="fixture")
        self.assertEqual(s.native_candidates([event()],[milestone()],entities=entities)[0]["reason"], "missing_native_participant_entity")
        wrong = {**row,"id":"native-B","type":"basketball_team","details":{"league":"NBA"}}
        entities = s.entity_identities([row,wrong],evidence="fixture")
        self.assertEqual(s.native_candidates([event()],[milestone()],entities=entities)[0]["reason"], "native_participant_entity_sport_disagreement")

    def test_alias_receipts_bind_entity_fields_and_sources_without_admitting_sports(self):
        entities = s.entity_identities([
            {"id":"native-A","name":"ALP Wolves","type":"football_team","details":{"league":"NFL","market":"Alpha"}},
            {"id":"native-B","name":"Beta Bears","type":"football_team","details":{"league":"NFL"}}],evidence="fixture native reference")
        games = s.provider_games([provider(names=("Alpha Wolves","Beta Bears"))])
        record = alias("ALP Wolves","Alpha Wolves",native_entity_id="native-A",review_id="fixture-review-1",
            native_source={"path":"structured_targets.parquet","sha256":"a"*64,"fields":["id","name","details.market"]},
            provider_source={"path":"provider_games.parquet","sha256":"b"*64,"fields":["participant_names"]})
        receipt = s.alias_receipts([record],native_entities=entities,providers=games)
        native = s.native_candidates([event()],[milestone()],entities=entities)
        self.assertEqual(s.bind_candidates(native,games,reviewed_aliases=receipt["aliases"])[0]["provider_game_id"],"P1")
        self.assertFalse(receipt["sports_admitted"])
        with self.assertRaisesRegex(s.BindingBlocked,"fingerprint"):
            s.alias_receipts([{**record,"native_source":{}}],native_entities=entities,providers=games)
        with self.assertRaisesRegex(s.BindingBlocked,"native literal"):
            s.alias_receipts([{**record,"native_name":"Guessed school"}],native_entities=entities,providers=games)

    def test_soccer_kind_plus_verified_epl_milestone_supplies_league_context(self):
        entities=s.entity_identities([
            {"id":"native-A","name":"Alpha FC","type":"soccer_team","details":{"short_name":"Alpha"}},
            {"id":"native-B","name":"Beta FC","type":"soccer_team","details":{"short_name":"Beta"}}],evidence="fixture native reference")
        m=milestone(details={"away_team_id":"native-A","home_team_id":"native-B","league":"English Premier"})
        native=s.native_candidates([event(series="KXEPLGAME")],[m],entities=entities)
        self.assertIsNone(native[0]["reason"])
        games=s.provider_games([provider(sport="epl")])
        self.assertEqual(s.bind_candidates(native,games)[0]["provider_game_id"],"P1")


class ResultMapTests(unittest.TestCase):
    def fixtures(self, sport="nfl", conflict=False):
        kind, league = ("soccer_team", "EPL") if sport == "epl" else ("football_team", "NFL")
        entities=s.entity_identities([{"id":mid,"name":name,"type":kind,"details":{"league":league}}
            for mid,name in (("native-A","Alpha"),("native-B","Beta"))],evidence="pinned fixture identities")
        native=s.native_candidates([event(series="KXEPLGAME" if sport=="epl" else "KXNFLGAME")],
            [milestone(details={"away_team_id":"native-A","home_team_id":"native-B","league":league})],entities=entities)
        game=provider(sport=sport,away_name="Beta" if conflict else "Alpha",home_name="Alpha" if conflict else "Beta")
        binding=s.bind_candidates(native,s.provider_games([game]))
        market={"ticker":"M1","event_ticker":"NATIVE-1","sport":sport,"market_reason":"eligible","status":"finalized",
            "market_type":"binary","combo_state":"single","unit_payoff_admitted":True,"face_value_dollars":1.,
            "result":"yes","title":"Alpha wins","yes_sub_title":"Alpha"}
        source={"path":"fixture_provider.json","sha256":"c"*64}
        result={"sport":sport,"provider_game_id":"P1","completed":True,"reason":None,"is_draw":False,
            "winner_participant_id":"P-A","participants":[{"id":"P-A","name_options":["Alpha"]},{"id":"P-B","name_options":["Beta"]}],
            "source_binding":source,"source_pointer":"events[0].competitions[0]"}
        prop={"ticker":"M1","kind":"named_participant","native_participant_id":"native-A","field":"yes_sub_title","text":"Alpha",
            "reviewed":True,"source_binding":{"path":"fixture_native.parquet","sha256":"d"*64}}
        return market,binding,result,prop

    def test_completed_named_yes_map_and_canonical_receipt_hashes(self):
        market,binding,result,prop=self.fixtures()
        output=s.prove_market_rows([market],binding,[result],[prop])
        self.assertEqual(len(output["market_map"]),1)
        row,proof=output["market_map"][0],output["binding_proofs"][0]
        self.assertEqual(row["yes_payout"],1)
        self.assertTrue(row["native_yes_result_verified"])
        self.assertEqual(s.hashlib.sha256(proof["result_receipt_json"].encode()).hexdigest(),row["result_receipt_sha256"])
        self.assertEqual(output["independent_acceptance"],"not_created")
        bad=s.prove_market_rows([{**market,"result":"no"}],binding,[result],[prop])
        self.assertEqual(bad["exclusions"][0]["reason"],"native_provider_yes_result_disagreement")

    def test_orientation_conflict_named_yes_allowed_but_relative_yes_not(self):
        market,binding,result,prop=self.fixtures(conflict=True)
        self.assertEqual(len(s.prove_market_rows([market],binding,[result],[prop])["market_map"]),1)
        relative=s.prove_market_rows([market],binding,[result],[{**prop,"kind":"home"}])
        self.assertEqual(relative["exclusions"][0]["reason"],"home_away_relative_proposition_orientation_unproved")

    def test_epl_draw_and_losing_named_claim_both_require_native_result_agreement(self):
        market,binding,result,prop=self.fixtures(sport="epl")
        result.update(is_draw=True,winner_participant_id=None)
        market.update(yes_sub_title="Draw")
        prop.update(kind="draw",text="Draw")
        self.assertEqual(s.prove_market_rows([market],binding,[result],[prop])["market_map"][0]["yes_payout"],1)
        market.update(yes_sub_title="Alpha",result="no")
        prop.update(kind="named_participant",text="Alpha")
        self.assertEqual(s.prove_market_rows([market],binding,[result],[prop])["market_map"][0]["yes_payout"],0)

    def test_unknown_winner_field_text_and_identity_do_not_become_losing_claims(self):
        market,binding,result,prop=self.fixtures()
        result["winner_participant_id"]=None
        self.assertEqual(s.prove_market_rows([market],binding,[result],[prop])["exclusions"][0]["reason"],"provider_winner_or_draw_identity_unproved")
        result["winner_participant_id"]="P-A"
        self.assertEqual(s.prove_market_rows([market],binding,[result],[{**prop,"text":"Beta"}])["exclusions"][0]["reason"],"native_yes_proposition_field_binding_differs")
        with self.assertRaisesRegex(s.BindingBlocked,"unique native market"):
            s.prove_market_rows([market,market],binding,[result],[prop])

    def test_literal_scoreboard_winner_draw_and_pending_status(self):
        def payload(flags=(True,False), scores=("2","1"), complete=True):
            return {"events":[{"competitions":[{"id":"G1","status":{"type":{"state":"post","completed":complete}},
                "competitors":[{"id":str(i),"team":{"id":str(i),"displayName":name,"shortDisplayName":name[:1]},
                    "winner":flags[i],"score":scores[i]} for i,name in enumerate(("Alpha","Beta"))]}]}]}
        source={"path":"saved_scoreboard.json","sha256":"a"*64}
        row=s.scoreboard_results(payload(),"epl",source_binding=source)[0]
        self.assertEqual(row["winner_participant_id"],"0")
        self.assertIn("A",row["participants"][0]["name_options"])
        draw=s.scoreboard_results(payload((False,False),("2","2")),"epl",source_binding=source)[0]
        self.assertTrue(draw["is_draw"])
        missing=s.scoreboard_results(payload((True,None)),"cbb",source_binding=source)[0]
        self.assertEqual(missing["reason"],"provider_result_winner_flags_missing")
        pending=s.scoreboard_results(payload(complete=False),"cbb",source_binding=source)[0]
        self.assertEqual(pending["reason"],"provider_result_not_completed")

    def test_native_yes_parser_uses_repeated_yes_subtitle_not_no_label_or_order(self):
        market,binding,result,_=self.fixtures()
        entities=s.entity_identities([{"id":mid,"name":name,"type":"football_team","details":{"league":"NFL"}}
            for mid,name in (("native-A","Alpha"),("native-B","Beta"))],evidence="fixture native identities")
        sources={"M1":{"path":"native_sports.parquet","sha256":"a"*64}}
        market.update(title="Beta at Alpha Winner?",no_sub_title="Alpha")
        parsed=s.native_yes_propositions([market],binding,[result],entities,source_bindings=sources)
        self.assertEqual(parsed["propositions"][0]["native_participant_id"],"native-A")
        self.assertEqual(parsed["propositions"][0]["field"],"yes_sub_title")
        bad=s.native_yes_propositions([{**market,"title":"Alpha loses"}],binding,[result],entities,source_bindings=sources)
        self.assertEqual(bad["exclusions"][0]["reason"],"unadmitted_native_yes_winner_title_syntax_or_labels")
        relative=s.native_yes_propositions([{**market,"yes_sub_title":"Home"}],binding,[result],entities,source_bindings=sources)
        self.assertEqual(relative["exclusions"][0]["reason"],"native_yes_label_not_unique_proved_participant")

    def test_completed_snapshot_conflicts_never_choose_majority_winner(self):
        _,_,result,_=self.fixtures()
        rows=s.reconcile_result_snapshots([result,{**result,"source_pointer":"events[1]"}])
        self.assertIsNone(rows[0]["reason"])
        conflict=s.reconcile_result_snapshots([result,{**result,"winner_participant_id":"P-B"}])
        self.assertEqual(conflict[0]["reason"],"conflicting_completed_provider_result_snapshots")
        scored={**result,"participants":[{**person,"score":i} for i,person in enumerate(result["participants"])]}
        changed={**scored,"participants":[{**person,"score":9} for person in scored["participants"]]}
        self.assertEqual(s.reconcile_result_snapshots([scored,changed])[0]["reason"],"conflicting_completed_provider_result_snapshots")

    def test_malformed_status_and_boolean_provider_ids_are_not_completed_proofs(self):
        source={"path":"fixture.json","sha256":"c"*64}
        competition={"id":"P1","status":{"type":"post"},"competitors":[
            {"team":{"id":"A","displayName":"Alpha"},"winner":True},
            {"team":{"id":"B","displayName":"Beta"},"winner":False}]}
        row=s.scoreboard_results({"events":[{"competitions":[competition]}]},"cbb",source_binding=source)[0]
        self.assertEqual(row["reason"],"provider_result_not_completed")
        competition["competitors"][0]["team"]["id"]=True
        row=s.scoreboard_results({"events":[{"competitions":[competition]}]},"cbb",source_binding=source)[0]
        self.assertEqual(row["reason"],"provider_result_participant_identity_missing")

    def test_atp_only_inspected_full_match_grammar_not_first_set_or_first_x(self):
        market,binding,result,_=self.fixtures()
        market.update(sport="atp")
        binding[0].update(sport="atp",timing_qualification="scheduled_start_plus_archived_duration")
        result.update(sport="atp")
        entities=s.entity_identities([{"id":mid,"name":name,"type":"tennis_competitor","details":{"gender":"male"}}
            for mid,name in (("native-A","Alpha"),("native-B","Beta"))],evidence="pinned fixture tennis identities")
        sources={"M1":{"path":"native.parquet","sha256":"a"*64}}
        for title in ("Alpha wins", "Will Alpha win the Beta vs Alpha: Round Of 128 match?",
                "Will Alpha be a winner of the round of 64 of Wimbledon Men Singles?"):
            with self.subTest(admitted=title):
                market["title"]=title
                parsed=s.native_yes_propositions([market],binding,[result],entities,source_bindings=sources)
                self.assertEqual(len(parsed["propositions"]),1)
                self.assertEqual(len(s.prove_market_rows([market],binding,[result],parsed["propositions"])["market_map"]),1)
        for title in ("Will Alpha win the first set of the match?", "Will Alpha win the first 5 games of the match?",
                "Will Alpha win the first set of Beta vs Alpha: Round Of 128 match?",
                "Will Alpha win the Gamma vs Delta: Round Of 128 match?",
                "Will Alpha be a winner of the round of 64 of the first set of Wimbledon Men Singles?"):
            with self.subTest(excluded=title):
                market["title"]=title
                parsed=s.native_yes_propositions([market],binding,[result],entities,source_bindings=sources)
                self.assertEqual(parsed["propositions"],[])
                self.assertEqual(parsed["exclusions"][0]["reason"],"unadmitted_native_yes_winner_title_syntax_or_labels")
                self.assertEqual(s.prove_market_rows([market],binding,[result],parsed["propositions"])["market_map"],[])

    def test_actual_scoreboard_completed_contradictions_block_with_source_receipts(self):
        def snapshot(flags, *, complete=True, name="valid.json"):
            payload={"events":[{"competitions":[{"id":"C1","status":{"type":{"state":"post" if complete else "in","completed":complete}},
                "competitors":[{"team":{"id":participant,"displayName":name},"winner":flags[i],"score":str(10-i)}
                    for i,(participant,name) in enumerate((("A","Alpha"),("B","Beta")))]}]}]}
            return s.scoreboard_results(payload,"cbb",source_binding={"path":name,"sha256":"b"*64})[0]
        valid=snapshot((True,False))
        malformed=snapshot((True,True),name="both_winners.json")
        rows=s.reconcile_result_snapshots([valid,malformed])
        self.assertEqual(rows[0]["reason"],"contradictory_or_ambiguous_completed_provider_result_snapshot")
        self.assertEqual(len(rows[0]["completed_source_snapshots"]),2)
        self.assertEqual(rows[0]["completed_source_snapshots"][1]["source_binding"]["path"],"both_winners.json")
        self.assertEqual(rows[0]["completed_source_snapshots"][1]["reason"],"provider_result_winner_or_draw_ambiguous")
        changed=snapshot((False,True),name="changed_winner.json")
        self.assertEqual(s.reconcile_result_snapshots([valid,changed])[0]["reason"],"conflicting_completed_provider_result_snapshots")
        pending=snapshot((True,False),complete=False,name="pending.json")
        accepted=s.reconcile_result_snapshots([pending,valid])[0]
        self.assertIsNone(accepted["reason"])
        self.assertEqual(accepted["ignored_incomplete_source_snapshots"][0]["source_binding"]["path"],"pending.json")
        repeated=s.reconcile_result_snapshots([valid,snapshot((True,False),name="repeat.json")])[0]
        self.assertIsNone(repeated["reason"])
        self.assertEqual(len(repeated["completed_source_snapshots"]),2)
        market,binding,_,prop=self.fixtures()
        market["sport"]=binding[0]["sport"]="cbb"
        binding[0]["provider_game_id"]="C1"
        excluded=s.prove_market_rows([market],binding,rows,[prop])["exclusions"][0]
        self.assertEqual(excluded["reason"],"independent_provider_completed_result_missing")
        raw=excluded["provider_result_receipt_json"]
        self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(),excluded["provider_result_receipt_sha256"])
        self.assertEqual(json.loads(raw)["provider_result"]["completed_source_snapshots"],rows[0]["completed_source_snapshots"])
        unrelated=s.prove_market_rows([{**market,"status":"active"}],binding,rows,[prop])["exclusions"][0]
        self.assertIsNone(unrelated["provider_result_receipt_json"])
        self.assertIsNone(unrelated["provider_result_receipt_sha256"])

    def test_material_literal_provider_quality_is_visible_in_map_not_only_provenance(self):
        market,binding,result,prop=self.fixtures()
        binding[0]["raw_provider_timing_quality"]="sparse_live_play_timestamps"
        row=s.prove_market_rows([market],binding,[result],[prop])["market_map"][0]
        self.assertIn("provider_quality=sparse_live_play_timestamps",row["timing_quality"])
        market["sport"]=binding[0]["sport"]=result["sport"]="atp"
        binding[0]["timing_qualification"]="scheduled_start_plus_archived_duration"
        binding[0]["raw_provider_timing_quality"]="elapsed_thirds_from_espn_start_and_archived_duration"
        row=s.prove_market_rows([market],binding,[result],[prop])["market_map"][0]
        self.assertTrue(row["timing_quality"].startswith("scheduled_start_plus_archived_duration"))
        self.assertIn("elapsed_thirds_from_espn_start_and_archived_duration",row["timing_quality"])


class SportsMapStageTests(unittest.TestCase):
    """Exercise the unmodified production path with explicitly tiny bindings.

    Only host/runtime/source constants and pinned universe sizes are mocked;
    body/schema/hash/path/proposition/result/publication gates are real.
    """
    def setUp(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from scripts import build_kaushik_kalshi_inputs as runner
        from analysis.kaushik_polymarket_replication import build_inputs as io
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        self.runs=self.root/"runs"
        self.runs.mkdir()
        self.source=self.root/"source"
        self.source.mkdir()
        self.stage=self.runs/"metadata"
        self.stage.mkdir()
        self.head="1"*40
        self.snapshot={"head":self.head,"files":{"sports_binding.py":"2"*64}}
        self.io=io
        def parquet(path,records):
            path.parent.mkdir(parents=True,exist_ok=True)
            pq.write_table(pa.Table.from_pylist(records),path)
            return {"path":str(path),**io._output_info(path)}
        def text(path,value):
            path.parent.mkdir(parents=True,exist_ok=True)
            raw=json.dumps(value,sort_keys=True,allow_nan=False).encode()
            path.write_bytes(raw)
            return {"path":str(path),"bytes":len(raw),"sha256":hashlib.sha256(raw).hexdigest()}
        self.text,self.parquet=text,parquet
        market={"ticker":"M1","event_ticker":"NATIVE-1","sport":"nfl","market_reason":"eligible","status":"finalized",
            "market_type":"binary","combo_state":"single","unit_payoff_admitted":True,"face_value_dollars":1.,
            "result":"yes","title":"Alpha at Beta Winner?","yes_sub_title":"Alpha"}
        artifacts={}
        for role in ("metadata","sports","exclusions"):
            record=parquet(self.stage/"artifacts"/role/"shard.parquet",[market])
            artifacts[role+"_shard"]={**record,"path":str(Path(record["path"]).relative_to(self.stage))}
        self.health={"rows":1,"ticker_groups":1,"nonnull_ticker_groups":1,"duplicate_ticker_keys":0,"invalid_ticker_rows":0,
            "conflicting_result_keys":0,"conflicting_status_keys":0,"expected_source_shards":["markets/shard.parquet"],
            "observed_source_shards":["markets/shard.parquet"]}
        proof=text(self.stage/"global_metadata_proof.json",{"status":"structural_global_metadata_keys_passed","source":self.snapshot,"health":self.health})
        self.metadata={"schema_version":"kaushik_kalshi_metadata_stage_v1","status":"metadata_complete_pending_independent_audit",
            "source":self.snapshot,"counts":{"source_rows":1,"source_shards":1},"global_metadata_proof":{**proof,"path":"global_metadata_proof.json"},"artifacts":artifacts}
        events=parquet(self.source/"events.parquet",[event()])
        native_m=milestone()
        native_m.update(primary_event_tickers=json.dumps(native_m["primary_event_tickers"]),details=json.dumps(native_m["details"]),source_id="",source_ids="{}")
        milestones=parquet(self.source/"milestones.parquet",[native_m])
        entities=parquet(self.source/"entities.parquet",[{"id":mid,"name":name,"type":"football_team","details":json.dumps({"league":"NFL"}),"source_id":"","source_ids":"{}"}
            for mid,name in (("native-A","Alpha"),("native-B","Beta"))])
        time={"scheduled_start_utc":"2026-01-02T01:00:00Z","actual_start_utc":"2026-01-02T01:10:00Z","actual_end_utc":"2026-01-02T04:00:00Z"}
        timing=parquet(self.source/"timing.parquet",[{"sport":"cbb","event_slug":"cbb-fixture","game_id":"C1","market_date":"2026-01-02",
            "actual_start_utc":time["actual_start_utc"],"actual_end_utc":time["actual_end_utc"],"timing_quality":"fixture_observed_plays"}])
        proof=parquet(self.source/"oldproof.parquet",[{"sport":"cbb","event_slug":"cbb-fixture","eligible":True,"provider_competition_id":"C1",
            "provider_start_utc":time["scheduled_start_utc"],"provider_participants":"Gamma | Delta","provider_event_name":"Gamma at Delta"}])
        base={"official_date":"2026-01-02","away_team_id":"A","away_team_name":"Alpha","home_team_id":"B","home_team_name":"Beta","winning_team_id":"A",**time}
        nfl=parquet(self.source/"nfl.parquet",[{"game_id":"P1",**base}])
        nba=parquet(self.source/"nba.parquet",[{"game_id":"N1",**base}])
        mlb=parquet(self.source/"mlb.parquet",[{"game_pk":10,**base}])
        phase=parquet(self.source/"phase.parquet",[{"market_id":"old-market","game_pk":10,**{key:value for key,value in base.items() if key not in ("winning_team_id","scheduled_start_utc")},"price_must_not_read":1234}])
        providerbinding=text(self.source/"binding.json",{"schema_version":"kaushik_replication_sports_binding_v1","provider_cohort_admitted":True,
            "retained_identity_result_timing_proof":True,"inputs":[{**record,"role":role} for role,record in
            (("six_timing",timing),("six_proof",proof),("nfl_moneylines",nfl),("nba_moneylines",nba),("mlb_phase",phase))]})
        self.cache=self.root/"cache"
        payload={"events":[{"competitions":[{"id":"C1","status":{"type":{"state":"post","completed":True}},"competitors":[
            {"team":{"id":"G","displayName":"Gamma"},"winner":True,"score":"10"},
            {"team":{"id":"D","displayName":"Delta"},"winner":False,"score":"1"}]}]}]}
        scoreboard=text(self.cache/"scoreboards"/"cbb"/"20260102.json",payload)
        index=text(self.source/"provider_index.json",{"files":[{**scoreboard,"path":"scoreboards/cbb/20260102.json"}]})
        aliases=text(self.source/"aliases.json",{"schema_version":"kaushik_kalshi_sports_alias_semantic_review_v1",
            "status":"root_semantic_review_recorded_pending_field_reopen","approved_aliases":[]})
        self.files={"metadata_manifest":text(self.stage/"candidate_manifest.json",self.metadata),"native_events":events,
            "native_milestones":milestones,"native_entities":entities,"provider_binding":providerbinding,"provider_index":index,
            "mlb_results":mlb,"alias_receipt":aliases}
        self.config={"schema_version":"kaushik_kalshi_sports_map_build_config_v1","status":"reviewed","metadata_source_head":self.head,
            "provider_cache_root":str(self.cache),"timing_qualification":"Retained provider clocks; ATP uses scheduled start plus archived match duration.","files":self.files}
        self.stack=ExitStack()
        self.addCleanup(self.stack.close)
        for name,value in {"DATA_ROOT":self.root,"RUN_ROOT":self.runs,"REPO_ROOT":Path(s.__file__).resolve().parents[2],
                "PRODUCTION_PYTHON":Path(sys.executable),"EXPECTED_MLB_RETAINED_GAMES":1}.items():
            self.stack.enter_context(mock.patch.object(s,name,value))
        self.stack.enter_context(mock.patch("production_guard.require_production_host"))
        self.stack.enter_context(mock.patch.object(runner,"source_snapshot",return_value=self.snapshot))
        self.stack.enter_context(mock.patch.object(runner,"EXPECTED_METADATA_FILES",1))
        self.stack.enter_context(mock.patch.object(runner,"EXPECTED_METADATA_ROWS",1))
        self.stack.enter_context(mock.patch("shutil.disk_usage",return_value=type("Disk",(),{"free":100_000_000_000})()))

    def run_stage(self, name="sports-map"):
        record=self.text(self.source/"config.json",self.config)
        return s.build_sports_map_stage(config_path=record["path"],config_sha256=record["sha256"],output_dir=self.runs/name,expected_source_head=self.head)

    def test_guarded_metadata_stage_exact_proofs_atomic_reopen_no_acceptance(self):
        import pyarrow.parquet as pq
        result=self.run_stage()
        self.assertEqual(result["counts"],{"source_markets":1,"mapped_markets":1,"excluded_markets":0})
        target=self.runs/"sports-map"
        self.assertFalse((target/"acceptance.json").exists())
        self.assertEqual(result["status"],"sports_map_complete_pending_independent_audit")
        rows=pq.read_table(target/result["artifacts"]["market_map"]["path"]).to_pylist()
        proofs=pq.read_table(target/result["artifacts"]["binding_proofs"]["path"]).to_pylist()
        self.assertEqual(rows[0]["yes_payout"],1)
        self.assertEqual(hashlib.sha256(proofs[0]["identity_receipt_json"].encode()).hexdigest(),rows[0]["identity_receipt_sha256"])
        identity=json.loads(proofs[0]["identity_receipt_json"])
        self.assertEqual(identity["native_source_bindings"]["native_entities"],self.files["native_entities"])
        self.assertIn("identity_result_timing",identity["provider_source_bindings"])
        with self.assertRaisesRegex(s.BindingBlocked,"new top-level"):
            self.run_stage()

    def test_body_hash_source_runtime_and_disk_gates_fail_before_output(self):
        self.config["files"]["native_entities"]["sha256"]="f"*64
        with self.assertRaisesRegex(s.BindingBlocked,"body hash"):
            self.run_stage()
        self.assertFalse((self.runs/"sports-map").exists())
        with mock.patch("production_guard.require_production_host",side_effect=RuntimeError("fixture host rejected")):
            with self.assertRaisesRegex(RuntimeError,"host rejected"):
                self.run_stage()
        with mock.patch.object(s,"PRODUCTION_PYTHON",Path("/not-the-production-python")):
            with self.assertRaisesRegex(s.BindingBlocked,"canonical production Python"):
                self.run_stage()
        with mock.patch("shutil.disk_usage",return_value=type("Disk",(),{"free":21_999_999_999})()):
            with self.assertRaisesRegex(s.BindingBlocked,"floor reservation"):
                self.run_stage()

    def test_global_conflict_and_artifact_path_roles_fail_closed(self):
        self.metadata["artifacts"]["sports_shard"]["path"]="../source/events.parquet"
        self.files["metadata_manifest"]=self.text(self.stage/"candidate_manifest.json",self.metadata)
        with self.assertRaisesRegex(s.BindingBlocked,"role/path inventory"):
            self.run_stage()

    def test_source_drift_and_output_failure_are_not_accepted(self):
        from scripts import build_kaushik_kalshi_inputs as runner
        with mock.patch.object(runner,"source_snapshot",side_effect=[self.snapshot,{"head":"3"*40}]):
            with self.assertRaisesRegex(s.BindingBlocked,"source drift"):
                self.run_stage()
        with mock.patch.object(self.io,"atomic_publish",side_effect=OSError("fixture publication failed")):
            with self.assertRaisesRegex(OSError,"publication failed"):
                self.run_stage()
        failed=list(self.runs.glob(".sports-map.staging-*"))
        self.assertEqual(len(failed),1)
        self.assertEqual(json.loads((failed[0]/"failure.json").read_text())["status"],"sports_map_incomplete")
        self.assertFalse((self.runs/"sports-map").exists())

    def test_guarded_empty_map_preserves_result_failure_receipt(self):
        import pyarrow.parquet as pq
        path=self.stage/"artifacts"/"sports"/"shard.parquet"
        rows=pq.read_table(path).to_pylist()
        rows[0]["result"]="no"
        record=self.parquet(path,rows)
        self.metadata["artifacts"]["sports_shard"]={**record,"path":"artifacts/sports/shard.parquet"}
        self.files["metadata_manifest"]=self.text(self.stage/"candidate_manifest.json",self.metadata)
        result=self.run_stage()
        self.assertEqual(result["counts"],{"source_markets":1,"mapped_markets":0,"excluded_markets":1})
        target=self.runs/"sports-map"
        self.assertEqual(pq.read_table(target/"sports_market_map.parquet").num_rows,0)
        self.assertEqual(pq.read_table(target/"sports_binding_proofs.parquet").num_rows,0)
        exclusions=pq.read_table(target/"sports_binding_exclusions.parquet")
        self.assertEqual(len(exclusions.schema),6)
        self.assertEqual(exclusions.to_pylist()[0]["reason"],"native_provider_yes_result_disagreement")
        self.assertIsNotNone(exclusions.to_pylist()[0]["provider_result_receipt_json"])


if __name__ == "__main__":
    unittest.main()
