import cv2

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

from .find_crop import find_crop
from .xmp_utils import transform_corners_to_deskewed, build_xmp


class FindCrop(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            'Locates a cropped image within its uncropped original using template matching. '
            'Writes an XMP sidecar (crs: namespace) encoding the deskew angle and crop bounds '
            'sufficient to recreate the crop exactly.'
        )
        tags = ['image', 'crop', 'xmp', 'deskew', 'autocrop', 'tekst']
        dependencies = ['opencv-python', 'numpy']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.uncropped_path_prop = PropertyDescriptor(
            name='Uncropped Image Path',
            description='Absolute path to the uncropped source image.',
            required=True,
        )
        self.cropped_path_prop = PropertyDescriptor(
            name='Cropped Image Path',
            description='Absolute path to the cropped image to locate within the uncropped image.',
            required=True,
        )
        self.resize_factor_prop = PropertyDescriptor(
            name='Resize Factor',
            description='Downsample factor before template matching. Lower = faster, less precise. Typical: 0.1.',
            required=True,
            default_value='0.1',
            validators=[StandardValidators.NUMBER_VALIDATOR],
        )
        self.check_inverted_prop = PropertyDescriptor(
            name='Check Inverted',
            description='Also try colour-inverted template matching for images with inverted tones.',
            required=True,
            allowable_values=['true', 'false'],
            default_value='true',
        )
        self.descriptors = [
            self.uncropped_path_prop,
            self.cropped_path_prop,
            self.resize_factor_prop,
            self.check_inverted_prop,
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        try:
            uncropped_path = (
                context.getProperty(self.uncropped_path_prop.name)
                .evaluateAttributeExpressions(flowfile).getValue()
            )
            cropped_path = (
                context.getProperty(self.cropped_path_prop.name)
                .evaluateAttributeExpressions(flowfile).getValue()
            )
            resize_factor = float(context.getProperty(self.resize_factor_prop.name).getValue())
            check_inverted = (
                context.getProperty(self.check_inverted_prop.name).getValue().lower() == 'true'
            )

            uncropped = cv2.imread(uncropped_path)
            if uncropped is None:
                self.logger.error(f'Cannot read uncropped image: {uncropped_path}')
                return FlowFileTransformResult(relationship='failure')

            cropped = cv2.imread(cropped_path)
            if cropped is None:
                self.logger.error(f'Cannot read cropped image: {cropped_path}')
                return FlowFileTransformResult(relationship='failure')

            result = find_crop(
                uncropped, cropped,
                resize_factor=resize_factor,
                check_inverted=check_inverted,
            )

            img_h, img_w = uncropped.shape[:2]
            top, left, bottom, right = transform_corners_to_deskewed(
                result.corners, result.rotation, img_w, img_h,
            )
            xmp = build_xmp(result.rotation, top, left, bottom, right)

            return FlowFileTransformResult(
                relationship='success',
                contents=xmp.encode('utf-8'),
                attributes={
                    'crop.score':    str(round(float(result.score),    6)),
                    'crop.rotation': str(round(float(result.rotation), 6)),
                },
            )
        except Exception as e:
            self.logger.error(f'FindCrop failed: {e}')
            return FlowFileTransformResult(relationship='failure')
