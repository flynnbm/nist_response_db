# ui_config.py
#
# Controls how each table is displayed in the card grid and modal view.
#
# card_fields:   Ordered list of fields to show on the card face.
#                Fields are shown in this order; blank values are skipped.
#                If a listed field doesn't exist in the row, it's ignored.
#                Falls back to first N populated fields if none match.
#
# modal_groups:  Named sections shown in the modal, in order.
#                Each group has a "label" and a list of "fields".
#                Any fields NOT listed in any group are automatically
#                collected into an "Other" section at the bottom,
#                so no data is ever lost.

TABLE_CONFIG = {

    "dexterity": {
        "card_fields": ["dexterity_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Info",   
             "fields": [
                 "date", 
                 "facility", 
                 "location", 
                 "administrator",
                 "operator_org"
             ]
             },
            {"label": "Robot Configuration",
             "fields": [
                 "robot_make",
                 "robot_model",
                 "robot_config",
             ]
             },
            {"label": "Test Configuration",
             "fields": [
                 "dexterity_test_method_full_name",
                 "dexterity_test_method_designation_version",
                 "dexterity_test_method_short_name",
                 "dexterity_test_method_type",
                 "apparatus_dimensions_apparatus_clearance_width_w",
                 "dexterity_inspect_target_pipes_length_l",
                 "dexterity_orientation",
                 "dexterity_terrain",
                 "dexterity_artifacts_type",
                 "dexterity_touch_and_insert_tool_shaft_length_and_hole_depth_l",
                 "dexterity_linear_or_omnidirectional_confinement",
                 "dexterity_linear_or_omnidirectional_height_depth_increments_0_2",
                 "dexterity_rotate_handle_diameter_d",
                 "dexterity_test_configuration",
                 "linear_or_omnidirectional_depth",
                 "linear_or_omnidirectional_height"
             ]
             },
            {"label": "Environment",
             "fields": [
                 "environment_temperature_degrees_celsius",
                 "environment_humidity",
                 "environment_environment_lighting_lux"
             ]
             },
            {"label": "Results",
             "fields": [
                 "time_min",
                 "inspect_completeness",
                 "average_acuity_mm",
                 "touch_completeness",
                 "insert_completeness",
                 "rotate_completeness",
                 "extract_completeness",
                 "place_completeness"
             ]
             }
            # Add more groups here as you learn the columns, e.g.:
            # {"label": "Performance", "fields": ["grip_force", "accuracy", "dof"]},
            # {"label": "Test Setup",  "fields": ["operator", "task_type", "environment"]},
        ],
    },

    "energy_power": {
        "card_fields": ["energy_power_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Identity",   "fields": ["robot_make", "robot_model", "date", "facility"]},
            # {"label": "Power",      "fields": ["battery_type", "capacity_wh", "runtime_min"]},
            # {"label": "Efficiency", "fields": ["peak_power_w", "idle_power_w", "charge_time"]},
        ],
    },

    "human_system_interaction": {
        "card_fields": ["human_system_interaction_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Identity",   "fields": ["robot_make", "robot_model", "date", "facility"]},
            # {"label": "Interaction","fields": ["interface_type", "modality", "response_time"]},
            # {"label": "Evaluation", "fields": ["usability_score", "operator", "task"]},
        ],
    },

    "mobility": {
        "card_fields": ["mobility_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Identity",   "fields": ["robot_make", "robot_model", "date", "facility"]},
            # {"label": "Locomotion", "fields": ["gait_type", "max_speed_ms", "terrain"]},
            # {"label": "Endurance",  "fields": ["distance_m", "duration_s", "incline_deg"]},
        ],
    },

    "radio_communications": {
        "card_fields": ["radio_communications_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Identity",   "fields": ["robot_make", "robot_model", "date", "facility"]},
            # {"label": "RF Details", "fields": ["frequency_mhz", "protocol", "range_m"]},
            # {"label": "Signal",     "fields": ["rssi_dbm", "packet_loss_pct", "latency_ms"]},
        ],
    },

    "robot_embodiment": {
        "card_fields": ["robot_embodiment_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Identity",   "fields": ["robot_make", "robot_model", "date", "facility"]},
            # {"label": "Physical",   "fields": ["height_m", "weight_kg", "dof", "form_factor"]},
            # {"label": "Actuation",  "fields": ["actuator_type", "num_joints", "payload_kg"]},
        ],
    },

    "sensing": {
        "card_fields": ["sensing_test_method_short_name","robot_make", "robot_model", "date", "facility"],
        "modal_groups": [
            {"label": "Identity",   "fields": ["robot_make", "robot_model", "date", "facility"]},
            # {"label": "Sensors",    "fields": ["sensor_type", "modality", "range_m", "fov_deg"]},
            # {"label": "Performance","fields": ["accuracy", "latency_ms", "update_rate_hz"]},
        ],
    },

}
