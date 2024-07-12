class Statistics:
    def __init__(self):
        self.flood_count = 0
        self.random_count = 0
        self.depth_count = 0
        self.flood_hops = []
        self.random_hops = []
        self.depth_hops = []

    def add_flood_hops(self, hops: int):
        self.flood_hops.append(hops)

    def add_random_hops(self, hops: int):
        self.random_hops.append(hops)

    def add_depth_hops(self, hops: int):
        self.depth_hops.append(hops)

    def calculate_stats(self, hops: list[int]) -> str:
        if not hops:
            return "N/A"
        avg = sum(hops) / len(hops)
        variance = sum((x - avg) ** 2 for x in hops) / len(hops)
        stdev = variance ** 0.5
        return f"{avg:.3} (dp {stdev:.3})"
