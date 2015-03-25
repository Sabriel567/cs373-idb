
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
    # Athletes model
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

# ----
# main
# ----

if __name__ == "__main__":
    main()
