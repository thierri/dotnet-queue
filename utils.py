def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def free_slots(pattern, position = 0):
    free_slots = 0

    for char in pattern[position:]:
        if not RepresentsInt(char):
            free_slots += 1
    return free_slots

def hour_sum(pattern, position = 0):
    hour_sum = 0

    for char in pattern[position:]:
        if RepresentsInt(char):
            hour_sum += int(char)
    return hour_sum

def max_allowed_value(pattern, max_week_hours, max_day_hours):
    remaining_hours = max_week_hours - hour_sum(pattern)
    if remaining_hours < max_day_hours:
        return remaining_hours
    else:
        return max_day_hours

def min_allowed_value(pattern, max_week_hours, max_day_hours):
    if free_slots(pattern) == 0:
        return max_week_hours - hour_sum(pattern)
    else:
        if (max_week_hours - hour_sum(pattern) - ((free_slots(pattern) - 1) * max_day_hours)) < 0:
            return 0
        else:
            return (max_week_hours - hour_sum(pattern) - ((free_slots(pattern) - 1) * max_day_hours)) 
