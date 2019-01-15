from utils import free_slots, hour_sum, RepresentsInt, max_allowed_value, min_allowed_value


def find_remaing_number(pattern, max_week_hours, max_day_hours):
    # print("Pattern sendo avaliado: " + pattern)
    for index, char in enumerate(pattern):
        if not RepresentsInt(char):
            _min = min_allowed_value(pattern, max_week_hours, max_day_hours)
            _max = max_allowed_value(pattern, max_week_hours, max_day_hours)
            if _min == _max:
                pattern = pattern.replace('?', str(_max), 1)
                if free_slots(pattern) == 0:
                    print(pattern)
                    # print("Soma: " + str(hour_sum(pattern)))
            else:
                for x in range(_min, _max + 1):
                    if free_slots(pattern.replace("?", str(x), 1)) == 0:
                        print(pattern.replace("?", str(x), 1))
                        # print("Soma: " + str(hour_sum(pattern.replace("?", str(x), 1))))
                    else:
                        find_remaing_number(pattern.replace("?", str(x), 1), max_week_hours, max_day_hours)
            break
        
def main():
    max_week_hours = 7
    max_day_hours = 2
    pattern = "1111???"
    
    find_remaing_number(pattern, max_week_hours, max_day_hours)

if __name__ == "__main__":
    main()
