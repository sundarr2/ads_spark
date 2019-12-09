import pytest

from ads_spark.spark import get_spark
from ads_spark.user_nextpage import ads_processing

class TestNextPageVisit:

    def test_transform(self):
        source_data = [
            ("event_1", "1527836939669", "VIEW", "visitor_1", "page1"), ## Visitor1
            ("event_2", "1527836940111", "VIEW", "visitor_1", "page1"), ## Visitor1 - Same Page in the next event - Ignore in the result
            ("event_3", "1527836941111", "VIEW", "visitor_1", "page2"), ## Visitor1
            ("event_4", "1527836942111", "VIEW", "visitor_2", "page1"), ## Visitor2
            ("event_5", "1527836943111", "VIEW", "visitor_1", "page1"), ## Visitor1
            ("event_6", "1527836944111", "VIEW", None, "page"),         ##          Null visitor Id - Ignore in the result
            ("event_7", "1527836945111", "VIEW", "visitor_3", "page1"), ## Visitor3 -
            ("event_8", "1527836946111", "VIEW", "visitor_2", "page1"), ## Visitor2 - ame Page in the next event  - Ignore in the result
            ("event_9", "1527836947111", "VIEW", None, "page"),         ##          Null visitor Id - Ignore in the result
            ("event_10", "1527836948111", "VIEW", "visitor_3", "page2"),## Visitor3
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["id", "timestamp","type","visitorId","pageUrl"]
        )

        actual_df = ads_processing.transform(ads_processing,source_df)

        expected_data = [
            # Visitor1
            ("event_1", 1527836939669, "VIEW", "visitor_1", "page1", "page2"),
            ("event_3", 1527836941111, "VIEW", "visitor_1", "page2", "page1"),
            ("event_5", 1527836943111, "VIEW", "visitor_1", "page1",None),
            # Visitor2
            ("event_4", 1527836942111, "VIEW", "visitor_2", "page1", None),
            # Visitor3
            ("event_7", 1527836945111, "VIEW", "visitor_3", "page1", "page2"),
            ("event_10", 1527836948111, "VIEW", "visitor_3", "page2", None)
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["id", "timestamp","type","visitorId","pageUrl","nextPageUrl"]
        )
        print(sorted(expected_df.collect()))
        print(sorted(actual_df.collect()))
        assert(sorted(expected_df.collect() )== sorted(actual_df.collect()))