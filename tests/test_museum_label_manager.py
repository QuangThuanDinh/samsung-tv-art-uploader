import csv
import json
import logging
import os
import tempfile
import unittest
from unittest import mock

from PIL import Image

from loop.MuseumLabelManager import MuseumLabelManager


class MuseumLabelManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.env = mock.patch.dict(
            os.environ,
            {'SAMSUNG_TV_ART_MUSEUM_LABEL': 'true'},
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.manager = MuseumLabelManager(
            media_root=self.temp_dir.name,
            log=logging.getLogger('test'),
        )

    def test_render_preserves_dimensions_and_mockup_geometry(self):
        source = os.path.join(self.temp_dir.name, 'painting.png')
        Image.new('RGB', (2000, 1000), color='black').save(source)

        destination = self.manager.process_image(
            source,
            {
                'artwork_title': 'Path in the Wheat at Pourville',
                'artwork_description': 'Claude Monet, 1882',
                'copyrightlink': 'https://example.com/art',
            },
        )

        self.assertEqual(
            os.path.basename(destination),
            'painting.museum-label.jpg',
        )
        with Image.open(destination) as output:
            self.assertEqual(output.size, (2000, 1000))
            pixels = output.load()
            changed = [
                (x, y)
                for y in range(output.height)
                for x in range(output.width)
                if max(pixels[x, y]) > 40
            ]
        left = min(x for x, _y in changed)
        top = min(y for _x, y in changed)
        right = max(x for x, _y in changed)
        bottom = max(y for _x, y in changed)
        self.assertGreaterEqual(left / 2000, 0.706)
        self.assertAlmostEqual(top / 1000, 0.878, delta=0.006)
        self.assertLessEqual((right - left + 1) / 2000, 0.255)
        self.assertAlmostEqual((bottom - top + 1) / 1000, 0.084, delta=0.006)

    def test_copyright_credit_is_split_onto_its_own_line(self):
        self.assertEqual(
            self.manager._split_copyright(
                'Sunrise in Redwood Parks (© HadelProductions/Getty Images)'
            ),
            (
                'Sunrise in Redwood Parks',
                '© HadelProductions/Getty Images',
            ),
        )
        self.assertEqual(
            self.manager._split_copyright('Artwork description ((c) Artist Name)'),
            ('Artwork description', 'c Artist Name'),
        )

    def test_short_content_uses_narrower_dynamic_box(self):
        short_image = Image.new('RGB', (2000, 1000), color='black')
        long_image = Image.new('RGB', (2000, 1000), color='black')

        self.manager._render(short_image, 'A', 'B', 'https://example.com')
        self.manager._render(
            long_image,
            'A considerably longer artwork title',
            'A considerably longer artwork description',
            'https://example.com',
        )

        def changed_width(image):
            bounds = image.getbbox()
            return bounds[2] - bounds[0]

        self.assertLess(changed_width(short_image), changed_width(long_image))

    def test_dynamic_box_rounds_up_to_preserve_full_title_width(self):
        fixed_width = 208.5
        title_width = 518

        box_width = self.manager._dynamic_box_width(
            fixed_width,
            title_width,
            max_box_width=3600,
        )

        self.assertGreaterEqual(box_width - fixed_width, title_width)

    def test_git_processing_is_idempotent_and_preserves_source(self):
        repository = os.path.join(self.temp_dir.name, 'Monet')
        os.makedirs(os.path.join(repository, '.git'))
        source = os.path.join(repository, 'painting.png')
        Image.new('RGB', (1200, 800), color='blue').save(source)
        with open(
            os.path.join(repository, 'artwork_data.csv'),
            'w',
            encoding='utf-8',
            newline='',
        ) as output:
            writer = csv.DictWriter(
                output,
                fieldnames=[
                    'artwork_file',
                    'artwork_title',
                    'artwork_description',
                    'copyrightlink',
                ],
            )
            writer.writeheader()
            writer.writerow({
                'artwork_file': 'painting.png',
                'artwork_title': 'Water Lilies',
                'artwork_description': 'Claude Monet',
                'copyrightlink': '',
            })

        with self.assertLogs('test.MuseumLabel', level='INFO') as logs:
            first = self.manager.process_git_collections()
        second = self.manager.process_git_collections()

        derivative = os.path.join(repository, 'painting.museum-label.jpg')
        self.assertTrue(os.path.isfile(source))
        self.assertTrue(os.path.isfile(derivative))
        self.assertEqual(first['processed'], 1)
        self.assertTrue(any(
            'Museum Label generating 1/1: Monet/painting.png' in message
            for message in logs.output
        ))
        self.assertEqual(second['processed'], 0)
        self.assertEqual(second['unchanged'], 1)
        self.assertEqual(
            self.manager.preferred_filenames(
                repository,
                ['painting.png', 'painting.museum-label.jpg'],
            ),
            ['painting.museum-label.jpg'],
        )

        before = os.path.getmtime(derivative)
        Image.new('RGB', (1200, 800), color='red').save(source)
        after_source_change = self.manager.process_git_collections()
        self.assertEqual(after_source_change['processed'], 0)
        self.assertEqual(after_source_change['unchanged'], 1)
        self.assertEqual(os.path.getmtime(derivative), before)

        regenerated = self.manager.regenerate_paths(
            ['Monet/painting.museum-label.jpg'],
            {
                'Monet/painting.png': {
                    'artwork_title': 'Water Lilies',
                    'artwork_description': 'Claude Monet',
                },
            },
            {},
        )
        self.assertEqual(
            [item['path'] for item in regenerated['generated']],
            ['Monet/painting.museum-label.jpg'],
        )
        self.assertEqual(regenerated['errors'], [])

    def test_render_version_change_regenerates_existing_derivative(self):
        repository = os.path.join(self.temp_dir.name, 'Monet')
        os.makedirs(os.path.join(repository, '.git'))
        source = os.path.join(repository, 'painting.png')
        Image.new('RGB', (1200, 800), color='blue').save(source)

        first = self.manager.process_git_collections()
        manifest_path = os.path.join(repository, '.museum-label-manifest.json')
        with open(manifest_path, 'r', encoding='utf-8') as manifest_file:
            manifest = json.load(manifest_file)
        manifest['version'] -= 1
        with open(manifest_path, 'w', encoding='utf-8') as manifest_file:
            json.dump(manifest, manifest_file)

        second = self.manager.process_git_collections()

        self.assertEqual(first['processed'], 1)
        self.assertEqual(second['processed'], 1)
        self.assertEqual(second['unchanged'], 0)

    def test_git_processing_skips_corrupt_image_and_continues(self):
        repository = os.path.join(self.temp_dir.name, 'Monet')
        os.makedirs(os.path.join(repository, '.git'))
        with open(os.path.join(repository, 'broken.jpg'), 'wb') as output:
            output.write(b'not an image')
        valid = os.path.join(repository, 'valid.png')
        Image.new('RGB', (1200, 800), color='blue').save(valid)

        result = self.manager.process_git_collections()

        self.assertEqual(result['processed'], 1)
        self.assertEqual(result['errors'], 1)
        self.assertFalse(
            os.path.exists(os.path.join(repository, 'broken.museum-label.jpg'))
        )
        self.assertTrue(
            os.path.isfile(os.path.join(repository, 'valid.museum-label.jpg'))
        )

    def test_disabled_mode_hides_generated_derivatives(self):
        with mock.patch.dict(
            os.environ,
            {'SAMSUNG_TV_ART_MUSEUM_LABEL': 'false'},
        ):
            manager = MuseumLabelManager(media_root=self.temp_dir.name)
        self.assertEqual(
            manager.preferred_filenames(
                self.temp_dir.name,
                ['painting.png', 'painting.museum-label.jpg'],
            ),
            ['painting.png'],
        )

    def test_normal_collection_uses_artist_year_and_google_query(self):
        title, description, target = self.manager._label_values(
            os.path.join(self.temp_dir.name, 'capri.jpg'),
            {
                'artwork_title': 'Capri',
                'artist_name': 'Albert Bierstadt',
                'artwork_year': '1957',
                'artwork_description': 'This text is not used',
                'copyrightlink': 'https://example.com/not-used',
            },
        )

        self.assertEqual(title, 'Capri')
        self.assertEqual(description, 'Albert Bierstadt / 1957')
        self.assertEqual(
            target,
            (
                'https://www.google.com/search?'
                'q=Capri+-+Albert+Bierstadt+%2F+1957'
            ),
        )

    def test_regenerate_derivative_uses_derivative_metadata(self):
        repository = os.path.join(self.temp_dir.name, 'Bing_DailyWallpaper')
        os.makedirs(repository)
        source = os.path.join(repository, 'daily.jpg')
        derivative = os.path.join(repository, 'daily.museum-label.jpg')
        Image.new('RGB', (1200, 800), color='blue').save(source)
        Image.new('RGB', (1200, 800), color='red').save(derivative)
        metadata = {
            'artwork_title': 'Crossing into history',
            'artwork_description': 'Brooklyn Bridge',
            'copyrightlink': 'https://example.com/bridge',
        }

        with mock.patch.object(
            self.manager,
            'process_image',
            wraps=self.manager.process_image,
        ) as process:
            result = self.manager.regenerate_paths(
                ['Bing_DailyWallpaper/daily.museum-label.jpg'],
                {
                    'Bing_DailyWallpaper/daily.museum-label.jpg': metadata,
                },
                {},
            )

        self.assertEqual(result['errors'], [])
        self.assertEqual(process.call_args.args[1], metadata)

    def test_bing_uses_copyright_description_and_link(self):
        title, description, target = self.manager._label_values(
            os.path.join(
                self.temp_dir.name,
                'Bing_DailyWallpaper',
                'daily.jpg',
            ),
            {
                'artwork_title': 'Crossing into history',
                'artist_name': 'Bing',
                'artwork_year': '2026',
                'artwork_description': 'Brooklyn Bridge, New York City',
                'copyrightlink': 'https://www.bing.com/search?q=Brooklyn+Bridge',
            },
        )

        self.assertEqual(title, 'Crossing into history')
        self.assertEqual(description, 'Brooklyn Bridge, New York City')
        self.assertEqual(
            target,
            'https://www.bing.com/search?q=Brooklyn+Bridge',
        )

    def test_collection_paths_include_every_preferred_image(self):
        repository = os.path.join(self.temp_dir.name, 'Monet')
        os.makedirs(repository)
        for filename in ('first.jpg', 'second.png'):
            Image.new('RGB', (20, 20), color='blue').save(
                os.path.join(repository, filename)
            )
        Image.new('RGB', (20, 20), color='red').save(
            os.path.join(repository, 'first.museum-label.jpg')
        )

        self.assertEqual(
            self.manager.paths_for_collections(['Monet']),
            [
                'Monet/first.museum-label.jpg',
                'Monet/second.png',
            ],
        )


if __name__ == '__main__':
    unittest.main()
