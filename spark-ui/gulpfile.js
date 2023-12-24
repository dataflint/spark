const gulp = require('gulp');
const inlinesource = require('gulp-inline-source');
const replace = require('gulp-replace');

gulp.task('default', () => {
return gulp
    .src('./build/*.html')
    .pipe(replace('.js"></script>', '.js" inline></script>'))
    .pipe(replace('<link rel="shortcut icon" href="./icon.png"/>', ''))
    .pipe(replace('<link rel="manifest" href="./manifest.json"/>', ''))
    .pipe(replace('rel="stylesheet">', 'rel="stylesheet" inline>'))
    .pipe(
    inlinesource({
        compress: false
    })
    )
    .pipe(gulp.dest('./build'));
});
