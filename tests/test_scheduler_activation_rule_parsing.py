from smart_common.repositories.scheduler_runtime_repository import _parse_activation_rule
from smart_common.schemas.automation_rule import (
    AutomationRuleCondition,
    AutomationRuleGroup,
)


def test_parse_activation_rule_accepts_nested_items() -> None:
    rule = _parse_activation_rule(
        {
            "operator": "ANY",
            "items": [
                {
                    "operator": "ALL",
                    "items": [
                        {
                            "source": "provider_primary_power",
                            "comparator": "gte",
                            "value": 2.0,
                            "unit": "W",
                        },
                        {
                            "source": "provider_primary_power",
                            "comparator": "gte",
                            "value": 2.0,
                            "unit": "W",
                        },
                    ],
                },
                {
                    "operator": "ANY",
                    "items": [
                        {
                            "source": "provider_battery_soc",
                            "comparator": "gte",
                            "value": 30.0,
                            "unit": "%",
                        },
                        {
                            "source": "provider_battery_soc",
                            "comparator": "gte",
                            "value": 30.0,
                            "unit": "%",
                        },
                    ],
                },
            ],
        }
    )

    assert isinstance(rule, AutomationRuleGroup)
    assert rule.operator == "ANY"
    assert len(rule.items or []) == 2
    assert isinstance(rule.items[0], AutomationRuleGroup)
    assert isinstance(rule.items[1], AutomationRuleGroup)
    assert isinstance(rule.items[0].items[0], AutomationRuleCondition)
    assert isinstance(rule.items[1].items[0], AutomationRuleCondition)
