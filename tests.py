
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

    # -------------
    # Athlete model
    # -------------

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

    # -----------
    # Medal model
    # -----------

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

    # -------------
    # Olympic model
    # -------------

    def test_olympic_has_id(self):
        self.assertTrue(hasattr(db.Olympics, "id"))

    def test_olympic_has_city_id(self):
        self.assertTrue(hasattr(db.Olympics, "city_id"))

    def test_olympic_has_year(self):
        self.assertTrue(hasattr(db.Olympics, "year"))

    def test_olympic_has_season(self):
        self.assertTrue(hasattr(db.Olympics, "season"))

    def test_olympics_populated(self):
        all_olympics = self.session.query(db.Olympics)\
                                    .select_from(db.Olympics)\
                                    .all()

        self.assertTrue(len(all_olympics) > 0)

    def test_olympic_id_not_null(self):
        olympic_ids = self.session.query(db.Olympics)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.id == None)\
                                    .all()
        
        self.assertTrue(len(olympic_ids) == 0)

    def test_olympic_city_id_not_null(self):
        olympic_city_ids = self.session.query(db.Olympics)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.city_id == None)\
                                    .all()

        self.assertTrue(len(olympic_city_ids) == 0)

    def test_olympic_year_not_null(self):
        olympic_years = self.session.query(db.Olympics)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.year == None)\
                                    .all()

        self.assertTrue(len(olympic_years) == 0)

    def test_olympic_season_not_null(self):
        olympic_seasons = self.session.query(db.Olympics)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.season == None)\
                                    .all()

        self.assertTrue(len(olympic_seasons) == 0)

    def test_olympic_id_1(self):
        olympic = self.session.query(db.Olympics.city_id,
                                        db.Olympics.year,
                                        db.Olympics.season)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.id == 11)\
                                    .all()[0]
        
        self.assertEqual(olympic[0], 37)
        self.assertEqual(olympic[1], 2008)
        self.assertEqual(olympic[2], "Summer")

    def test_olympic_id_2(self):
        olympic = self.session.query(db.Olympics.city_id,
                                        db.Olympics.year,
                                        db.Olympics.season)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.id == 8)\
                                    .all()[0]
        
        self.assertEqual(olympic[0], 35)
        self.assertEqual(olympic[1], 1996)
        self.assertEqual(olympic[2], "Summer")

    def test_olympic_id_3(self):
        olympic = self.session.query(db.Olympics.city_id,
                                        db.Olympics.year,
                                        db.Olympics.season)\
                                    .select_from(db.Olympics)\
                                    .filter(db.Olympics.id == 4)\
                                    .all()[0]
        
        self.assertEqual(olympic[0], 31)
        self.assertEqual(olympic[1], 1980)
        self.assertEqual(olympic[2], "Summer")
        
    # ----------
    # City model
    # ----------
    
    def test_city_has_id(self):
        self.assertTrue(hasattr(db.City, "id"))

    def test_city_has_name(self):
        self.assertTrue(hasattr(db.City, "name"))

    def test_city_has_country_id(self):
        self.assertTrue(hasattr(db.City, "country_id"))
        
    def test_city_populated(self):
        all_cities = self.session.query(db.City)\
                                    .select_from(db.City)\
                                    .all()

        self.assertTrue(len(all_cities) > 0)

    def test_city_id_not_null(self):
        city_ids = self.session.query(db.City)\
                                    .select_from(db.City)\
                                    .filter(db.City.id == None)\
                                    .all()
        
        self.assertTrue(len(city_ids) == 0)

    def test_city_name_not_null(self):
        city_names = self.session.query(db.City)\
                                    .select_from(db.City)\
                                    .filter(db.City.name == None)\
                                    .all()

        self.assertTrue(len(city_names) == 0)

    def test_city_country_id_not_null(self):
        country_id = self.session.query(db.City)\
                                    .select_from(db.City)\
                                    .filter(db.City.country_id == None)\
                                    .all()

        self.assertTrue(len(country_id) == 0)
        
    def test_city_id_1(self):
        city = self.session.query(db.City.name,
                                    db.City.country_id)\
                                .select_from(db.City)\
                                .filter(db.City.id == 1)\
                                .all()[0]
        
        self.assertEqual(city[0], "Athens")
        self.assertEqual(city[1], 76)

    def test_city_id_2(self):
        city = self.session.query(db.City.name,
                                        db.City.country_id)\
                                    .select_from(db.City)\
                                    .filter(db.City.id == 28)\
                                    .all()[0]
        
        self.assertEqual(city[0], "Mexico")
        self.assertEqual(city[1], 122)

    def test_city_id_3(self):
        city = self.session.query(db.City.name,
                                        db.City.country_id)\
                                    .select_from(db.City)\
                                    .filter(db.City.id == 37)\
                                    .all()[0]
    
        self.assertEqual(city[0], "Beijing")
        self.assertEqual(city[1], 44)

    # -----------
    # Event model
    # -----------
        
    def test_event_has_id(self):
        self.assertTrue(hasattr(db.Event, "id"))

    def test_event_has_sport_id(self):
        self.assertTrue(hasattr(db.Event, "sport_id"))

    def test_event_has_name(self):
        self.assertTrue(hasattr(db.Event, "name"))

    def test_event_has_gender(self):
        self.assertTrue(hasattr(db.Event, "gender"))
        
    def test_event_populated(self):
        all_events = self.session.query(db.Event)\
                                    .select_from(db.Event)\
                                    .all()

        self.assertTrue(len(all_events) > 0)

    def test_event_id_not_null(self):
        event_ids = self.session.query(db.Event)\
                                    .select_from(db.Event)\
                                    .filter(db.Event.id == None)\
                                    .all()
        
        self.assertTrue(len(event_ids) == 0)

    def test_event_sport_id_not_null(self):
        event_sport_ids = self.session.query(db.Event)\
                                    .select_from(db.Event)\
                                    .filter(db.Event.sport_id == None)\
                                    .all()

        self.assertTrue(len(event_sport_ids) == 0)

    def test_event_name_not_null(self):
        event_names = self.session.query(db.Event)\
                                    .select_from(db.Event)\
                                    .filter(db.Event.name == None)\
                                    .all()

        self.assertTrue(len(event_names) == 0)

    def test_event_gender_not_null(self):
        event_gender = self.session.query(db.Event)\
                                    .select_from(db.Event)\
                                    .filter(db.Event.gender == None)\
                                    .all()

        self.assertTrue(len(event_gender) == 0)
        
    def test_event_id_1(self):
        event = self.session.query(db.Event.sport_id,
                                    db.Event.name,
                                    db.Event.gender)\
                                .select_from(db.Event)\
                                .filter(db.Event.id == 219)\
                                .all()[0]
        
        self.assertEqual(event[0], 3)
        self.assertEqual(event[1], "10000m")
        self.assertEqual(event[2], "Men")

    def test_event_id_2(self):
        event = self.session.query(db.Event.sport_id,
                                    db.Event.name,
                                    db.Event.gender)\
                                .select_from(db.Event)\
                                .filter(db.Event.id == 76)\
                                .all()[0]
        
        self.assertEqual(event[0], 35)
        self.assertEqual(event[1], "25m rapid fire pistol (60 shots)")
        self.assertEqual(event[2], "Men")

    def test_event_id_3(self):
        event = self.session.query(db.Event.sport_id,
                                    db.Event.name,
                                    db.Event.gender)\
                                .select_from(db.Event)\
                                .filter(db.Event.id == 570)\
                                .all()[0]

        self.assertEqual(event[0], 15)
        self.assertEqual(event[1], "individual road race")
        self.assertEqual(event[2], "Women")

    # -----------
    # Sport model
    # -----------

    
    def test_sport_has_id(self):
        self.assertTrue(hasattr(db.Sport, "id"))

    def test_sport_has_name(self):
        self.assertTrue(hasattr(db.Sport, "name"))

    def test_sport_id_not_null(self):
        sport_ids = self.session.query(db.Sport)\
                                    .select_from(db.Sport)\
                                    .filter(db.Sport.id == None)\
                                    .all()

        self.assertTrue(len(sport_ids) == 0)

    def test_sport_name_not_null(self):
        sport_names = self.session.query(db.Event)\
                                    .select_from(db.Event)\
                                    .filter(db.Sport.name == None)\
                                    .all()

        self.assertTrue(len(sport_names) == 0)

    def test_sport_id_1(self):
        sport = self.session.query(db.Sport.name)\
                                .select_from(db.Sport)\
                                .filter(db.Sport.id == 3)\
                                .all()[0]
        
        self.assertEqual(sport[0], "Athletics")

    def test_sport_id_2(self):
        sport = self.session.query(db.Sport.name)\
                                .select_from(db.Sport)\
                                .filter(db.Sport.id == 15)\
                                .all()[0]
        
        self.assertEqual(sport[0], "Cycling")

    def test_sport_id_3(self):
        sport = self.session.query(db.Sport.name)\
                                .select_from(db.Sport)\
                                .filter(db.Sport.id == 25)\
                                .all()[0]
        
        self.assertEqual(sport[0], "Judo")

# ----
# main
# ----

if __name__ == "__main__":
    main()
