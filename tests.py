
# -------
# imports
# -------

from unittest import main, TestCase
import models as db

# ----------
# TestModels
# ----------

class TestModels(TestCase):
    
    # --------------
    # setup/teardown
    # --------------

    def setUp(self):
        self.session = db.loadSession()

    def tearDown(self):
        self.session.close()

    # --------------
    # Athlete model
    # --------------

    def test_athlete_has_id(self):
        self.assertTrue(hasattr(db.Athlete, "id"))

    def test_athlete_has_first_name(self):
        self.assertTrue(hasattr(db.Athlete, "first_name"))

    def test_athlete_has_last_name(self):
        self.assertTrue(hasattr(db.Athlete, "last_name"))

    def test_athlete_has_gender(self):
        self.assertTrue(hasattr(db.Athlete, "gender"))

    def test_athletes_populated(self):
        all_athletes = self.session.query(db.Athlete)\
                                    .select_from(db.Athlete)\
                                    .all()

        self.assertTrue(len(all_athletes) > 0)

    def test_athlete_id_not_null(self):
        athlete_ids = self.session.query(db.Athlete)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.id == None)\
                                    .all()
        
        self.assertTrue(len(athlete_ids) == 0)

    def test_athlete_first_name_not_null(self):
        athlete_first_names = self.session.query(db.Athlete)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.first_name == None)\
                                    .all()

        self.assertTrue(len(athlete_first_names) == 0)

    def test_athlete_last_name_not_null(self):
        athlete_last_names = self.session.query(db.Athlete)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.last_name == None)\
                                    .all()

        self.assertTrue(len(athlete_last_names) == 0)

    def test_athlete_gender_not_null(self):
        athlete_genders = self.session.query(db.Athlete)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.gender == None)\
                                    .all()

        self.assertTrue(len(athlete_genders) == 0)

    def test_athlete_id_1(self):
        athlete = self.session.query(db.Athlete.first_name,
                                            db.Athlete.last_name,
                                            db.Athlete.gender)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.id == 13839)\
                                    .all()[0]
        
        self.assertEqual(athlete[0], "Monique")
        self.assertEqual(athlete[1], "Knol")
        self.assertEqual(athlete[2], "Women")

    def test_athlete_id_2(self):
        athlete = self.session.query(db.Athlete.first_name,
                                            db.Athlete.last_name,
                                            db.Athlete.gender)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.id == 9493)\
                                    .all()[0]

        self.assertEqual(athlete[0], "Emma")
        self.assertEqual(athlete[1], "Gapchenko")
        self.assertEqual(athlete[2], "Women")

    def test_athlete_id_3(self):
        athlete = self.session.query(db.Athlete.first_name,
                                            db.Athlete.last_name,
                                            db.Athlete.gender)\
                                    .select_from(db.Athlete)\
                                    .filter(db.Athlete.id == 9492)\
                                    .all()[0]

        self.assertEqual(athlete[0], "Doreen Viola Hansen")
        self.assertEqual(athlete[1], "Wilber")
        self.assertEqual(athlete[2], "Women")

    # -------------
    # Country model
    # -------------

    def test_country_has_id(self):
        self.assertTrue(hasattr(db.Country, "id"))

    def test_country_has_name(self):
        self.assertTrue(hasattr(db.Country, "name"))

    def test_country_has_noc(self):
        self.assertTrue(hasattr(db.Country, "noc"))

    def test_countries_populated(self):
        all_countries = self.session.query(db.Country)\
                                    .select_from(db.Country)\
                                    .all()

        self.assertTrue(len(all_countries) > 0)

    def test_country_id_not_null(self):
        country_ids = self.session.query(db.Country)\
                                    .select_from(db.Country)\
                                    .filter(db.Country.id == None)\
                                    .all()
        
        self.assertTrue(len(country_ids) == 0)

    def test_country_name_not_null(self):
        country_names = self.session.query(db.Country)\
                                    .select_from(db.Country)\
                                    .filter(db.Country.name == None)\
                                    .all()

        self.assertTrue(len(country_names) == 0)

    def test_country_noc_not_null(self):
        country_nocs = self.session.query(db.Country)\
                                    .select_from(db.Country)\
                                    .filter(db.Country.noc == None)\
                                    .all()

        self.assertTrue(len(country_nocs) == 0)

    def test_country_id_1(self):
        country = self.session.query(db.Country.name,
                                            db.Country.noc)\
                                    .select_from(db.Country)\
                                    .filter(db.Country.id == 195)\
                                    .all()[0]
        
        self.assertEqual(country[0], "United States")
        self.assertEqual(country[1], "USA")

    def test_country_id_2(self):
        country = self.session.query(db.Country.name,
                                            db.Country.noc)\
                                    .select_from(db.Country)\
                                    .filter(db.Country.id == 96)\
                                    .all()[0]
        
        self.assertEqual(country[0], "Japan")
        self.assertEqual(country[1], "JPN")

    def test_country_id_3(self):
        country = self.session.query(db.Country.name,
                                            db.Country.noc)\
                                    .select_from(db.Country)\
                                    .filter(db.Country.id == 74)\
                                    .all()[0]
        
        self.assertEqual(country[0], "Germany")
        self.assertEqual(country[1], "GER")

    # --------------
    # Medal model
    # --------------

    def test_medal_has_id(self):
        self.assertTrue(hasattr(db.Medal, "id"))

    def test_medal_has_rank(self):
        self.assertTrue(hasattr(db.Medal, "rank"))

    def test_medal_has_athlete_id(self):
        self.assertTrue(hasattr(db.Medal, "athlete_id"))

    def test_medal_has_country_id(self):
        self.assertTrue(hasattr(db.Medal, "country_id"))

    def test_medal_has_event_id(self):
        self.assertTrue(hasattr(db.Medal, "event_id"))

    def test_medal_has_olympic_id(self):
        self.assertTrue(hasattr(db.Medal, "olympic_id"))

    def test_medals_populated(self):
        all_medals = self.session.query(db.Medal)\
                                    .select_from(db.Medal)\
                                    .all()

        self.assertTrue(len(all_medals) > 0)

    def test_medal_id_not_null(self):
        medal_ids = self.session.query(db.Medal)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.id == None)\
                                    .all()
        
        self.assertTrue(len(medal_ids) == 0)

    def test_medal_rank_not_null(self):
        medal_rank = self.session.query(db.Medal.rank)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.rank == None)\
                                    .all()

        self.assertTrue(len(medal_rank) == 0)


    def test_medal_country_id_not_null(self):
        medal_country_id = self.session.query(db.Medal)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.country_id == None)\
                                    .all()

        self.assertTrue(len(medal_country_id) == 0)

    def test_medal_olympics_id_not_null(self):
        medal_olympics_id = self.session.query(db.Medal)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.olympic_id == None)\
                                    .all()

        self.assertTrue(len(medal_olympics_id) == 0)

    def test_medal_event_id_not_null(self):
        medal_event_id = self.session.query(db.Medal)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.event_id == None)\
                                    .all()

        self.assertTrue(len(medal_event_id) == 0)

    def test_medal_athlete_id_not_null(self):
        medal_athlete_id = self.session.query(db.Medal)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.athlete_id == None)\
                                    .all()

        self.assertTrue(len(medal_athlete_id) == 0)

    def test_medal_id_1(self):
        medal = self.session.query(db.Medal.rank,
                                            db.Medal.athlete_id,
                                            db.Medal.country_id,
                                            db.Medal.olympic_id,
                                            db.Medal.event_id)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.id == 3)\
                                    .all()[0]
        
        self.assertEqual(medal[0], "Silver")
        self.assertEqual(medal[1], 8695)
        self.assertEqual(medal[2], 195)
        self.assertEqual(medal[3], 1)
        self.assertEqual(medal[4], 84)

    def test_medal_id_2(self):
        medal = self.session.query(db.Medal.rank,
                                            db.Medal.athlete_id,
                                            db.Medal.country_id,
                                            db.Medal.olympic_id,
                                            db.Medal.event_id)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.id == 1212)\
                                    .all()[0]
        
        self.assertEqual(medal[0], "Gold")
        self.assertEqual(medal[1], 9492)
        self.assertEqual(medal[2], 195)
        self.assertEqual(medal[3], 2)
        self.assertEqual(medal[4], 500)

    def test_medal_id_3(self):
        medal = self.session.query(db.Medal.rank,
                                            db.Medal.athlete_id,
                                            db.Medal.country_id,
                                            db.Medal.olympic_id,
                                            db.Medal.event_id)\
                                    .select_from(db.Medal)\
                                    .filter(db.Medal.id == 5383)\
                                    .all()[0]
        
        self.assertEqual(medal[0], "Bronze")
        self.assertEqual(medal[1], 12609)
        self.assertEqual(medal[2], 56)
        self.assertEqual(medal[3], 5)
        self.assertEqual(medal[4], 371)


# ----
# main
# ----

if __name__ == "__main__":
    main()
