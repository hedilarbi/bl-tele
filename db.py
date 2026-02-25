from db_core.config import DB_FILE, VEHICLE_CLASSES
from db_core.schema import init_db, _add_column, _ensure_tg_user_columns
from db_core.users import (
    upsert_user_from_bot,
    add_user,
    update_token,
    set_token_status,
    update_portal_token,
    get_portal_token,
    get_mobile_headers,
    get_mobile_auth,
    get_token_status,
    update_filters,
    get_all_users,
    get_all_users_with_bot_admin_active,
    get_user_row,
    get_active,
    set_active,
    get_user_timezone,
    set_user_timezone,
    get_notifications,
    set_notification,
    set_bl_account,
    get_bl_account,
    get_bl_account_full,
    set_bl_uuid,
    get_bl_uuid,
)
from db_core.bots import (
    add_bot_instance,
    delete_bot_instance,
    list_bot_instances,
    get_bot_instance,
    get_bot_token,
    list_bots_for_user,
    assign_bot_owner,
    set_bot_admin_active,
    get_bot_admin_active,
)
from db_core.slots import add_booked_slot, get_booked_slots, delete_booked_slot
from db_core.schedule import get_blocked_days, add_blocked_day, delete_blocked_day
from db_core.vehicles import get_vehicle_classes_state, toggle_vehicle_class
from db_core.offer_messages import save_offer_message, get_offer_message
from db_core.offer_logs import (
    log_offer_decision,
    get_processed_offer_ids,
    get_offer_logs,
    get_offer_logs_counts,
    get_offer_stats,
)
from db_core.pinned_warnings import get_pinned_warnings, save_pinned_warning, clear_pinned_warning
from db_core.custom_filters import (
    create_custom_filter,
    list_all_custom_filters,
    get_filter_by_slug,
    update_custom_filter,
    assign_custom_filter,
    unassign_custom_filter,
    toggle_user_custom_filter,
    list_user_custom_filters,
)
from db_core.endtime_formulas import (
    get_endtime_formulas,
    replace_endtime_formulas,
    add_endtime_formula,
    delete_endtime_formula,
    get_user_endtime_formulas,
)
