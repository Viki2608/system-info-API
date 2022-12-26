# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestSysteminfoController(BaseTestCase):
    """SysteminfoController integration test stubs"""

    def test_systeminfo_webhook(self):
        """Test case for systeminfo_webhook

        adds new system info
        """
        response = self.client.open(
            '/gcm',
            method='POST',
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
