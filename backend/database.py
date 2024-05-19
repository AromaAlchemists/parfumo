import pandas as pd


# sample code for test
def get_data(recommendations):
    sample_data = {
        "perfume_id": [1, 2, 3, 4, 5],
        "perfume_name": [
            "Gentleman Givenchy Réserve Privée",
            "City of Stars",
            "Acqua di Giò pour Homme Eau de Parfum",
            "Apex",
            "Terre d'Hermès Eau Givrée",
        ],
        "year": [2022, 2022, 2022, 2022, 2022],
        "brand": [
            "Givenchy",
            "Louis Vuitton",
            "Giorgio Armani",
            "Roja Parfums",
            "Hermès",
        ],
        "image_url": [
            "https://pimages.parfumo.de/480/158077_img-4050-givenchy-gentleman-givenchy-eau-de-parfum-reserve-privee_480.jpg",
            "https://pimages.parfumo.de/480/167670_img-2443-louis-vuitton-city-of-stars_480.jpg",
            "https://pimages.parfumo.de/480/160300_img-8398-giorgio-armani-acqua-di-gio-pour-homme-eau-de-parfum_480.jpg",
            "https://pimages.parfumo.de/480/165208_img-6247-roja-parfums-apex_480.jpg",
            "https://pimages.parfumo.de/480/164275_img-5053-hermes-terre-d-hermes-eau-givree_480.jpg",
        ],
        "notes_top": [
            ["Italian bergamot"],
            [],
            ["Italian green mandarin orange", "Marine notes"],
            ["Lemon", "Bergamot", "Mandarin orange", "Orange"],
            [],
        ],
        "notes_heart": [
            ["Chestnut", "Benzoin Siam"],
            [],
            ["Provençal clary sage", "Provençal lavender", "Madagascan geranium"],
            ["Jasmine", "Cistus", "Pineapple"],
            [],
        ],
        "notes_base": [
            ["Virginia cedar", "Haitian vetiver", "Indonesian patchouli"],
            [
                "Blood orange",
                "Lemon",
                "Red mandarin orange",
                "Bergamot",
                "Lime",
                "Tiaré",
                "Sandalwood",
                "Musk",
            ],
            ["Guatemala patchouly", "Haitian vetiver"],
            [
                "Galbanum",
                "Elemi resin",
                "Patchouli",
                "Oakmoss",
                "Rum",
                "Tobacco",
                "Cypress",
                "Fir balsam",
                "Juniper berry",
                "Cashmere wood",
                "Sandalwood",
                "Benzoin",
                "Amber",
                "Frankincense",
                "Labdanum",
                "Leather",
                "Ambergris",
                "Musk",
            ],
            ["Citron", "Juniper berry", "Nepalese Sichuan pepper"],
        ],
        "rating": [8.1, 7.2, 7.2, 7, 7.5],
        "link": [
            "https://www.parfumo.net/Perfumes/Givenchy/gentleman-givenchy-reserve-privee",
            "https://www.parfumo.net/Perfumes/Louis_Vuitton/city-of-stars",
            "https://www.parfumo.net/Perfumes/Giorgio_Armani/acqua-di-gio-pour-homme-eau-de-parfum",
            "https://www.parfumo.net/Perfumes/Roja_Parfums/apex",
            "https://www.parfumo.net/Perfumes/Hermes/terre-d-hermes-eau-givree",
        ],
    }

    df = pd.DataFrame(sample_data)

    filtered_df = df[df["perfume_name"].isin(recommendations)]

    return filtered_df
