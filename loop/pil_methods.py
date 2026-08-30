"""Pillow-based image synchronization helpers for the TV uploader."""

import io
import logging
import os

HAVE_PIL = False
try:
    from PIL import Image
    try:
        from PIL import ImageChops, ImageFilter  # type: ignore
    except Exception:
        ImageChops = None
        ImageFilter = None
    HAVE_PIL = True
except Exception:
    Image = None
    ImageChops = None
    ImageFilter = None


class PIL_methods:
    
    def __init__(self, mon):
        self.log = logging.getLogger('Main.'+__class__.__name__)
        self.mon = mon
        self.uploaded_files = self.mon.uploaded_files
        
    async def initialize(self):
        '''
        initialize uploaded_files using PIL
        compares the file data with thumbnails to find the content_id and write to uploaded_files
        if it doesn't already exist
        '''
        if not HAVE_PIL:
            return True
        self.log.info('Checking uploaded files list using PIL')
        files_images = self.load_files()
        if files_images:
            self.log.info('getting My Photos list')
            my_photos = await self.mon.get_tv_content('MY-C0002')
            if my_photos is None:
                # None means the request failed. Reporting that as "no photos"
                # states a fact we never established and hides a dead channel.
                self.log.warning(
                    'could not read My Photos from TV; skipping thumbnail sync'
                )
                return False
            if my_photos:
                await self.check_thumbnails(files_images, my_photos)
            else:
                self.log.info('no photos found on tv')
        else:
            self.log.info('no files, using origional uploaded files list')
        return True
            
    async def check_thumbnails(self, files_images, my_photos):
        '''
        download thumbnails from my_photos to compare with file data
        save any updates
        '''
        self.log.info('downloading My Photos thumbnails')
        my_photos_thumbnails = await self.get_thumbnails(my_photos)
        if my_photos_thumbnails:
            self.log.info('checking thumbnails against {} files, please wait...'.format(len(files_images)))
            self.compare_thumbnails(files_images, my_photos_thumbnails)
            self.mon.write_program_data()
        else:
            self.log.info('failed to get thumbnails')
            
    def compare_thumbnails(self, files_images, my_photos_thumbnails):
        '''
        compare file data with thumbnails to find a match, and update update_uploaded_files
        '''
        for k, (filename, file_data) in enumerate(files_images.items()):
            for i, (my_content_id, my_data) in enumerate(my_photos_thumbnails.items()):
                self.log_progress(len(files_images)*len(my_photos_thumbnails), k*len(files_images)+i)
                self.log.debug('checking: {} against {}, thumbnail: {} bytes'.format(filename, my_content_id, len(my_data)))
                if self.are_images_equal(Image.open(io.BytesIO(my_data)), file_data):
                    self.log.info('found uploaded file: {} as {}'.format(filename, my_content_id))
                    if filename not in self.uploaded_files.keys():
                        self.mon.update_uploaded_files(filename, my_content_id)
                    break
        
    def log_progress(self, total, count):
        '''
        log % progress every 10% if this will take a while
        '''
        if total >= 1000:
            percent = min(100,(count*100)//total)
            if count % (total//10) == 0:
                self.log.info('{}% complete'.format(percent))
        
    def load_files(self):
        '''
        reads folder files, and returns dictionary of filenames and binary data
        only used if PIL is installed
        '''
        files = self.mon.get_folder_files()
        self.log.info('loading files: {}'.format(files))
        files_images = self.get_files_dict(files)
        self.log.info('loaded: {}'.format(list(files_images.keys())))
        return files_images
        
    def get_files_dict(self, files):
        '''
        makes a dictionary of filename and file binary data
        warns if file type given by extension is wrong
        only used if PIL is installed
        '''
        files_images = {}
        folder = self.mon.folder
        for file in files:
            # Hard-skip any non-image artifacts like CSVs
            try:
                ext = os.path.splitext(file)[1].lower()
            except Exception:
                ext = ''
            if ext in ['.csv', '.json', '.jsonl', '.txt']:
                continue
            try:
                path = os.path.join(folder, file)
                data = Image.open(path)
                format = self.mon.get_file_type(path, data)
                if not (file.lower().endswith(format) or (format=='jpeg' and file.lower().endswith('jpg'))):
                    self.log.warning('file: {} is of type {}, the extension is wrong! please fix this'.format(file, format))
                files_images[file] = data
            except Exception as e:
                self.log.warning('Error loading: {}, {}'.format(file, e))
        return files_images
        
    async def get_thumbnails(self, content_ids):
        '''
        gets thumbnails from tv in list of content_ids
        returns dictionary of content_ids and binary data
        only used if PIL is installed

        Thumbnail fetches are best-effort: some legacy Frame TVs (e.g. Art API 1.07)
        return raw binary JPEG through a UTF-8/JSON decode path inside samsungtvws,
        which raises UnicodeDecodeError. We skip the offending thumbnail(s) instead of
        aborting the whole sync — otherwise the exception tears down the connection and
        can trigger a reconnect loop after uploads exist on the TV.
        '''
        thumbnails = {}
        if content_ids:
            if self.mon.api_version == 0:
                for content_id in content_ids:
                    try:
                        thumbnails[content_id] = await self.mon.tv.get_thumbnail(content_id)
                    except Exception as e:
                        self.log.warning('skipping thumbnail %s — could not fetch/decode: %s', content_id, e)
            elif self.mon.api_version == 1:
                try:
                    thumbnails = {os.path.splitext(k)[0]:v for k,v in (await self.mon.tv.get_thumbnail_list(content_ids)).items()}
                except Exception as e:
                    self.log.warning('failed to fetch thumbnail list (%s) — skipping thumbnail sync', e)
        self.log.info('got {} thumbnails'.format(len(thumbnails)))
        return thumbnails

    async def matches_file(self, content_id, path):
        """Return True/False when a TV thumbnail can be compared, otherwise None."""
        if not HAVE_PIL or not os.path.isfile(path):
            return None
        thumbnails = await self.get_thumbnails([content_id])
        thumbnail = thumbnails.get(content_id)
        if not thumbnail:
            return None
        try:
            with Image.open(io.BytesIO(thumbnail)) as tv_image:
                with Image.open(path) as local_image:
                    return self.are_images_equal(tv_image, local_image)
        except Exception as e:
            self.log.warning('failed to compare thumbnail %s with %s: %s', content_id, path, e)
            return None
        
    def fix_file_type(self, filename, file_type, image_data=None):
        if not all([HAVE_PIL, file_type]):
            return file_type
        org = file_type
        file_type = Image.open(filename).format.lower() if not image_data else image_data.format.lower()
        if file_type in['jpg', 'jpeg', 'mpo']:
            file_type = 'jpeg'
        if not (org == file_type or (org == 'jpg' and file_type == 'jpeg')):
            self.log.warning('file {} type changed from {} to {}'.format(filename, org, file_type))
        return file_type
        
    def are_images_equal(self, img1, img2):
        '''
        rough check if images are similar using PIL (avoid numpy which is faster)
        '''
        img1 = img1.convert('L').resize((384, 216)).filter(ImageFilter.GaussianBlur(radius=2))
        img2 = img2.convert('L').resize((384, 216)).filter(ImageFilter.GaussianBlur(radius=2))
        img3 = ImageChops.difference(img1, img2)    #updated 11/3/25 per suggestion in issue #11
        diff = sum(img3.get_flattened_data())/(384*216)  #normalize
        equal_content = diff <= 1.0                 #pick a threshhold
        self.log.debug('equal_content: {}, diff: {}'.format(equal_content, diff))
        return equal_content
