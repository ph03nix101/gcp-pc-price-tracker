# ==========================================
# MASTER CONFIGURATION
# Maps retailers → scraper functions → category URLs
# ==========================================

MASTER_CONFIG = {
    "Wootware": {
        "scraper_function": "scrape_wootware",
        "categories": {
            "GPUs": [
                "https://www.wootware.co.za/computer-hardware/video-cards-video-devices",
            ],
            "CPUs": [
                "https://www.wootware.co.za/computer-hardware/cpus-processors",
            ],
            "Laptops": [
                "https://www.wootware.co.za/pcs-and-laptops/laptops-notebooks",
            ],
            "SSDs": [
                "https://www.wootware.co.za/computer-hardware/hard-drives-ssds/solid-state-disks",
            ],
            "RAM": [
                "https://www.wootware.co.za/computer-hardware/memory-ram",
            ],
            "Motherboards": [
                "https://www.wootware.co.za/computer-hardware/motherboards",
            ],
        },
    },
    "Evetech": {
        "scraper_function": "scrape_evetech",
        "categories": {
            "GPUs": [
                "https://www.evetech.co.za/components/nvidia-ati-graphics-cards-21",
            ],
            "CPUs": [
                "https://www.evetech.co.za/components/buy-cpu-processors-online-164",
            ],
            "Laptops": [
                "https://www.evetech.co.za/laptop-specials-for-sale-south-africa",
            ],
            "SSDs": [
                "https://www.evetech.co.za/components/ssds-63",
            ],
            "RAM": [
                "https://www.evetech.co.za/components/ddr3-gaming-ram-modules-20",
            ],
            "Motherboards": [
                "https://www.evetech.co.za/components/cheap-intel-amd-based-motherboards-19",
            ],
        },
    },
    "Progenix": {
        "scraper_function": "scrape_progenix",
        "categories": {
            "GPUs": [
                "https://progenix.co.za/Components/Graphics-Cards/refine/stock_status,7?limit=100",
            ],
            "Motherboards": [
                "https://progenix.co.za/Components/Motherboards/refine/stock_status,7?limit=100",
            ],
            "CPUs": [
                "https://progenix.co.za/Components/CPUs/refine/stock_status,7?limit=100",
            ],
            "Laptops": [
                "https://progenix.co.za/Laptops/Laptops-Notebooks/refine/stock_status,7?limit=100",
            ],
            "SSDs": [
                "https://progenix.co.za/Components/Storage/refine/stock_status,7?limit=100",
            ],
            "RAM": [
                "https://progenix.co.za/Components/RAM-Memory/refine/stock_status,7?limit=100",
            ],
        },
    },
    "Computer Mania": {
        "scraper_function": "scrape_computermania",
        "categories": {
            "CPUs": [
                "https://computermania.co.za/collections/cpus",
            ],
            "Laptops": [
                "https://computermania.co.za/collections/laptops",
            ],
            "SSDs": [
                "https://computermania.co.za/collections/solid-state-drives",
            ],
            "RAM": [
                "https://computermania.co.za/collections/ram-2",
            ],
            "Motherboards": [
                "https://computermania.co.za/collections/motherboards",
            ],
            "GPUs": [
                "https://computermania.co.za/collections/graphics-cards",
            ],
        },
    },
    "Incredible Connection": {
        "scraper_function": "scrape_incredible",
        "categories": {
            "RAM": [
                "https://www.incredible.co.za/products/computers-printers-accessories/components/memory-ram",
            ],
            "SSDs": [
                "https://www.incredible.co.za/products/computers-printers-accessories/components/internal-hard-drives",
            ],
            "Laptops": [
                "https://www.incredible.co.za/products/computers-printers-accessories/laptops",
            ],
        },
    },
    "Dreamware Tech": {
        "scraper_function": "scrape_dreamware",
        "categories": {
            "GPUs": [
                "https://www.dreamwaretech.co.za/c/computer-components/graphics-cards-gpus/intel-arc-graphics-cards/",
                "https://www.dreamwaretech.co.za/c/computer-components/graphics-cards-gpus/workstation-cards/",
                "https://www.dreamwaretech.co.za/c/computer-components/graphics-cards-gpus/amd-graphics-cards/",
                "https://www.dreamwaretech.co.za/c/computer-components/graphics-cards-gpus/nvidia-graphics-cards/",
            ],
            "CPUs": [
                "https://www.dreamwaretech.co.za/c/computer-components/processors-cpus/intel-processors/",
                "https://www.dreamwaretech.co.za/c/computer-components/processors-cpus/amd-processors/",
            ],
            "Laptops": [
                "https://www.dreamwaretech.co.za/c/laptops-accessories/laptops-notebooks/",
            ],
            "RAM": [
                "https://www.dreamwaretech.co.za/c/computer-components/memory-ram/ddr4-desktop-pc-memory/",
                "https://www.dreamwaretech.co.za/c/computer-components/memory-ram/ddr3-desktop-pc-memory/",
                "https://www.dreamwaretech.co.za/c/computer-components/memory-ram/ddr5-desktop-memory/",
            ],
            "Motherboards": [
                "https://www.dreamwaretech.co.za/c/computer-components/motherboards/amd-motherboards/",
                "https://www.dreamwaretech.co.za/c/computer-components/motherboards/intel-motherboards/",
            ],
            "SSDs": [
                "https://www.dreamwaretech.co.za/c/computer-components/solid-state-drives-ssd/msata-solid-state-drives/",
                "https://www.dreamwaretech.co.za/c/computer-components/solid-state-drives-ssd/m-2-solid-state-drives/",
                "https://www.dreamwaretech.co.za/c/computer-components/solid-state-drives-ssd/2-5-solid-state-drives/",
            ],
        },
    },
    # "PC International": {
    #     "scraper_function": "scrape_pc_international",
    #     "categories": {
    #         "CPUs": [
    #             "https://pcinternational.co.za/product-category/computer-components/processors/",
    #         ],
    #         "Laptops": [
    #             "https://pcinternational.co.za/product-category/notebooks/",
    #         ],
    #         "SSDs": [
    #             "https://pcinternational.co.za/product-category/storage/",
    #         ],
    #         "RAM": [
    #             "https://pcinternational.co.za/product-category/computer-components/memory/",
    #         ],
    #         "Motherboards": [
    #             "https://pcinternational.co.za/product-category/computer-components/motherboard/",
    #         ],
    #         "GPUs": [
    #             "https://pcinternational.co.za/product-category/computer-components/all-graphics-cards/",
    #         ],
    #     },
    # },
}
