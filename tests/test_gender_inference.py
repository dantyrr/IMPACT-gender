import pytest
from src.pipeline.gender_inference import (
    extract_forename,
    GenderInferenceEngine,
    GenderResult,
)


class TestExtractForename:
    def test_standard_name(self):
        assert extract_forename("Gill, Natasha") == "Natasha"

    def test_initials_only(self):
        assert extract_forename("Curtis, M J") is None  # Too short

    def test_two_char_initials(self):
        assert extract_forename("Smith, MJ") is None

    def test_single_name(self):
        assert extract_forename("Madonna") is None  # No comma

    def test_empty(self):
        assert extract_forename("") is None
        assert extract_forename(None) is None

    def test_hyphenated_first(self):
        assert extract_forename("Kim, Soo-Hyun") == "Soo-Hyun"

    def test_compound_first(self):
        assert extract_forename("Crump, Aria Davis") == "Aria"

    def test_chinese_name(self):
        assert extract_forename("Wang, Xiaoming") == "Xiaoming"

    def test_single_initial_with_period(self):
        assert extract_forename("Smith, J.") is None


class TestGenderResult:
    def test_creation(self):
        r = GenderResult(gender="female", probability=0.95, count=5000)
        assert r.gender == "female"
        assert r.is_confident(threshold=0.60)

    def test_below_threshold(self):
        r = GenderResult(gender="male", probability=0.55, count=10)
        assert not r.is_confident(threshold=0.60)

    def test_to_code_female(self):
        r = GenderResult(gender="female", probability=0.95, count=100)
        assert r.to_code(0.60) == "W"

    def test_to_code_male(self):
        r = GenderResult(gender="male", probability=0.80, count=100)
        assert r.to_code(0.60) == "M"

    def test_to_code_unknown(self):
        r = GenderResult(gender=None, probability=0.0, count=0)
        assert r.to_code(0.60) == "U"

    def test_to_code_low_confidence(self):
        r = GenderResult(gender="female", probability=0.55, count=5)
        assert r.to_code(0.60) == "U"


class TestOfflineGenderEngine:
    def test_common_female(self):
        from src.pipeline.gender_inference import OfflineGenderEngine
        engine = OfflineGenderEngine()
        result = engine.infer("Sarah")
        assert result.gender == "female"
        assert result.probability >= 0.75

    def test_common_male(self):
        from src.pipeline.gender_inference import OfflineGenderEngine
        engine = OfflineGenderEngine()
        result = engine.infer("David")
        assert result.gender == "male"
        assert result.probability >= 0.75

    def test_unknown_name(self):
        from src.pipeline.gender_inference import OfflineGenderEngine
        engine = OfflineGenderEngine()
        result = engine.infer("Xyzzyplugh")
        assert result.gender is None

    def test_batch(self):
        from src.pipeline.gender_inference import OfflineGenderEngine
        engine = OfflineGenderEngine()
        results = engine.infer_batch(["Sarah", "David", "Xyzzyplugh"])
        assert len(results) == 3
        assert results["Sarah"].gender == "female"
        assert results["David"].gender == "male"
