from django.test import TestCase
from rest_framework.exceptions import ValidationError

from .models import Campaign, MessageContent, SUPPORTED_LANGUAGES
from .serializers import MessageContentSerializer


class MessageContentModelTests(TestCase):
    def test_default_content_created_with_campaign(self):
        campaign = Campaign.objects.create(name="foo")
        # signal or serializer should create the related object
        self.assertTrue(hasattr(campaign, "message_content"))
        content = campaign.message_content.content
        self.assertEqual(set(content.keys()), set(SUPPORTED_LANGUAGES))
        # all values should be strings (empty by default)
        for v in content.values():
            self.assertIsInstance(v, str)


class MessageContentSerializerTests(TestCase):
    def test_serializer_rejects_incomplete_languages(self):
        data = {"content": {"en": "a", "am": "b"}, "default_language": "en"}
        serializer = MessageContentSerializer(data=data)
        with self.assertRaises(ValidationError):
            serializer.is_valid(raise_exception=True)

    def test_serializer_accepts_exact_languages(self):
        good = {lang: "" for lang in SUPPORTED_LANGUAGES}
        data = {"content": good, "default_language": "en"}
        serializer = MessageContentSerializer(data=data)
        self.assertTrue(serializer.is_valid(), serializer.errors)


class CampaignAPITests(TestCase):
    def setUp(self):
        self.client = self.client  # django TestCase has client
        # create a base campaign via the serializer to guarantee message_content exists
        good = {lang: f"msg-{lang}" for lang in SUPPORTED_LANGUAGES}
        campaign_data = {
            "name": "test",
            "status": "draft",
            "schedule": {"start_date": "2026-01-01", "frequency": "daily", "run_days": [], "send_times": [], "end_times": []},
            "message_content": {"content": good, "default_language": "en"},
            "audience": {"recipients": []},
        }
        # try creating via both possible URL prefixes; real project may mount the
        # router under '/scheduler_manager/' or '/api/'.  tests will use whichever
        # one returns 201 first.
        for prefix in ['/scheduler_manager', '/api']:
            resp = self.client.post(prefix + '/campaigns/', campaign_data, content_type='application/json')
            if resp.status_code == 201:
                self.base_url = prefix
                break
        else:
            self.fail(f"failed to create campaign on either prefix: {resp.status_code} {resp.content}")
        self.campaign_id = resp.json()['id']

    def test_get_message_content_via_action(self):
        url = f'{self.base_url}/campaigns/{self.campaign_id}/message-content/'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        j = resp.json()
        self.assertIn('content', j)
        self.assertEqual(j['content']['en'], 'msg-en')

    def test_patch_message_content_via_action(self):
        url = f'{self.base_url}/campaigns/{self.campaign_id}/message-content/'
        new = {lang: f"new-{lang}" for lang in SUPPORTED_LANGUAGES}
        resp = self.client.patch(url, {'content': new}, content_type='application/json')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['content']['en'], 'new-en')

    def test_create_does_not_raise_duplicate_messagecontent(self):
        # the previous bug occurred when the signal and serializer both tried to
        # create the MessageContent row; creating another campaign should work
        good = {lang: f"f{lang}" for lang in SUPPORTED_LANGUAGES}
        new_campaign = {
            "name": "another",
            "status": "draft",
            "schedule": {"start_date": "2026-01-02", "frequency": "daily", "run_days": [], "send_times": [], "end_times": []},
            "message_content": {"content": good, "default_language": "en"},
            "audience": {"recipients": []},
        }
        resp = self.client.post(self.base_url + '/campaigns/', new_campaign, content_type='application/json')
        self.assertEqual(resp.status_code, 201, resp.content)

    def test_delete_cascades_related(self):
        # delete the first campaign and ensure related objects are removed
        url = f'{self.base_url}/campaigns/{self.campaign_id}/'
        resp = self.client.delete(url)
        self.assertEqual(resp.status_code, 204)
        # confirm related models are gone
        from .models import Schedule, MessageContent, Audience
        self.assertFalse(Schedule.objects.filter(campaign_id=self.campaign_id).exists())
        self.assertFalse(MessageContent.objects.filter(campaign_id=self.campaign_id).exists())
        self.assertFalse(Audience.objects.filter(campaign_id=self.campaign_id).exists())
