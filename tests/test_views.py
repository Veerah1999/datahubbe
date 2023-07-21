from .test_setup import Testsetup



class TestView(Testsetup):

    def test_user_can_register_with_no_data(self):
        res = self.client.post(self.register_url)
        self.assertEqual(res.status_code,200)