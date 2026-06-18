import cv2

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

from .find_crop import find_crop
from .xmp_utils import transform_corners_to_deskewed, snap_bounds_to_known_size, build_xmp
from .alto_utils import parse_alto_page_dimensions, alto_to_pixels, NB_ACCESS_SCALE


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
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        )
        self.cropped_path_prop = PropertyDescriptor(
            name='Cropped Image Path',
            description='Absolute path to the cropped image to locate within the uncropped image.',
            required=True,
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
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
        self.alto_path_prop = PropertyDescriptor(
            name='Alto XML Path',
            description=(
                'Optional path to the ALTO XML file for the cropped image. '
                'When set, the processor validates that the detected crop dimensions '
                'match the ALTO page dimensions within the configured tolerance. '
                'Routes to dimension_mismatch if they differ by more than the tolerance.'
            ),
            required=False,
            expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        )
        self.dimension_tolerance_prop = PropertyDescriptor(
            name='Dimension Tolerance',
            description=(
                'Maximum allowed pixel difference between detected crop dimensions '
                'and ALTO page dimensions. Only used when Alto XML Path is set.'
            ),
            required=True,
            default_value='200',
            validators=[StandardValidators.NUMBER_VALIDATOR],
        )
        self.use_hough_prop = PropertyDescriptor(
            name='Use Hough Estimation',
            description=(
                'Use Hough line detection to estimate the document skew angle before '
                'running the full rotation search. When confident, this seeds a narrow '
                '±2° search window instead of scanning the full ±5° grid, improving '
                'speed and extending effective range. Falls back to the full grid if '
                'Hough is not confident or the seeded match score is too low.'
            ),
            required=True,
            allowable_values=['true', 'false'],
            default_value='true',
        )
        self.descriptors = [
            self.uncropped_path_prop,
            self.cropped_path_prop,
            self.resize_factor_prop,
            self.check_inverted_prop,
            self.alto_path_prop,
            self.dimension_tolerance_prop,
            self.use_hough_prop,
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
            use_hough = (
                context.getProperty(self.use_hough_prop.name).getValue().lower() == 'true'
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
                use_hough=use_hough,
            )

            img_h, img_w = uncropped.shape[:2]
            top, left, bottom, right = transform_corners_to_deskewed(
                result.corners, result.rotation, img_w, img_h,
            )
            top, left, bottom, right = snap_bounds_to_known_size(
                top, left, bottom, right, cropped.shape[1], cropped.shape[0],
            )
            xmp = build_xmp(result.rotation, top, left, bottom, right)

            attributes = {
                'crop.score':      str(round(float(result.score),    6)),
                'crop.rotation':   str(round(float(result.rotation), 6)),
                'crop.used_hough': str(result.used_hough).lower(),
            }

            alto_path = (
                context.getProperty(self.alto_path_prop.name)
                .evaluateAttributeExpressions(flowfile).getValue()
            )
            if alto_path:
                try:
                    with open(alto_path, 'r', encoding='utf-8') as f:
                        alto_xml = f.read()
                    alto_w_mm10, alto_h_mm10 = parse_alto_page_dimensions(alto_xml)

                    expected_w = alto_to_pixels(alto_w_mm10, NB_ACCESS_SCALE)
                    expected_h = alto_to_pixels(alto_h_mm10, NB_ACCESS_SCALE)
                    found_w = right - left
                    found_h = bottom - top
                    error_w = abs(found_w - expected_w)
                    error_h = abs(found_h - expected_h)
                    tolerance = int(float(
                        context.getProperty(self.dimension_tolerance_prop.name).getValue()
                    ))
                    match = error_w <= tolerance and error_h <= tolerance

                    attributes['crop.dimension_match']   = str(match).lower()
                    attributes['crop.dimension_error_w'] = str(error_w)
                    attributes['crop.dimension_error_h'] = str(error_h)

                    if not match:
                        self.logger.error(
                            f'Crop dimension mismatch: found {found_w}x{found_h}, '
                            f'expected {expected_w}x{expected_h} (tolerance {tolerance}px)'
                        )
                        return FlowFileTransformResult(
                            relationship='dimension_mismatch',
                            contents=xmp.encode('utf-8'),
                            attributes=attributes,
                        )
                except Exception as e:
                    self.logger.error(f'ALTO validation failed: {e}')
                    return FlowFileTransformResult(relationship='failure')

            return FlowFileTransformResult(
                relationship='success',
                contents=xmp.encode('utf-8'),
                attributes=attributes,
            )
        except Exception as e:
            self.logger.error(f'FindCrop failed: {e}')
            return FlowFileTransformResult(relationship='failure')
