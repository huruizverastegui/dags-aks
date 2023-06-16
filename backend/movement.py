import random


def dummy_make_movement(areas):
    all_areas = [area["name"] for area in areas]
    target_areas = areas[: int(len(all_areas) / 2)]
    from random import choice, randrange

    movs = list()
    for area in target_areas:
        for i in range(0, len(all_areas) - 1):
            mov = dict()
            total_displacement = area["women"] * (random.randrange(0, 10) / 100)
            mov["origin"] = area["name"]
            mov["destination"] = choice(all_areas)
            mov["total"] = total_displacement * random.random()
            if mov["origin"] != mov["destination"]:
                movs.append(mov)
    return movs
