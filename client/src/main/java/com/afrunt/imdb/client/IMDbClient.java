package com.afrunt.imdb.client;

import com.afrunt.imdb.model.*;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

/**
 * @author Andrii Frunt
 */
public class IMDbClient {

    private InputStreamsProvider inputStreamsProvider;

    public IMDbClient() {
        this.inputStreamsProvider = new InputStreamsProvider();
    }

    public IMDbClient(InputStreamsProvider inputStreamsProvider) {
        this.inputStreamsProvider = inputStreamsProvider;
    }

    public Stream<Name> nameStream() {

        ModelConverter<Name> nameConverter = new ModelConverter<Name>("nconst") {
            @Override
            protected Name convert(String[] strings) {
                Name nm = new Name()
                        .setNameId(stringToId(strings[0], "nm"))
                        .setPrimaryName(strings[1].trim())
                        .setBirthYear(stringToInteger(strings[2]))
                        .setDeathYear(stringToInteger(strings[3]));

                if (strings.length > 4 && strings[4] != null) {
                    nm.setPrimaryProfessions(Arrays.stream(strings[4].split(",")).map(String::trim).collect(Collectors.toList()));
                }

                if (strings.length > 5 && strings[5] != null && !strings[5].trim().equals("\\N")) {
                    nm.setKnownForTitles(stringToIds(strings[5], "tt"));
                }

                return nm;
            }
        };


        return getModelStream(() -> inputStreamsProvider.namesInputStream(), nameConverter);
    }

    public Stream<TitleAkas> titleAkasStream() {
        ModelConverter<TitleAkas> titleAkasModelConverter = new ModelConverter<TitleAkas>("titleId") {
            @Override
            protected TitleAkas convert(String[] strings) {
                return new TitleAkas()
                        .setTitleId(stringToId(strings[0], "tt"))
                        .setOrdering(stringToInteger(strings[1]))
                        .setTitle(strings[2].trim())
                        .setRegion(whenAvailableString(strings[3]))
                        .setLanguage(whenAvailableString(strings[4]))
                        .setTypes(
                                whenAvailable(strings[5])
                                        .map(s -> Arrays.stream(s.split(",")).map(String::trim).collect(Collectors.toList()))
                                        .orElse(Collections.emptyList())
                        )
                        .setAttributes(whenAvailableString(strings[6]))
                        .setOriginalTitle(whenAvailable(strings[7]).map(s -> !"0".equals(s)).orElse(false))
                        ;
            }
        };
        return getModelStream(() -> inputStreamsProvider.titleAkasInputStream(), titleAkasModelConverter);
    }

    public Stream<TitleBasics> titleBasicsStream() {
        ModelConverter<TitleBasics> titleBasicsModelConverter = new ModelConverter<TitleBasics>("tconst") {
            @Override
            protected TitleBasics convert(String[] strings) {
                return new TitleBasics()
                        .setTitleId(stringToId(strings[0], "tt"))
                        .setTitleType(strings[1].trim())
                        .setPrimaryTitle(strings[2])
                        .setOriginalTitle(strings[3])
                        .setAdult(!"0".equals(strings[4].trim()))
                        .setStartYear(stringToInteger(strings[5]))
                        .setEndYear(stringToInteger(strings[6]))
                        .setRuntimeMinutes(stringToInteger(strings[7]))
                        .setGenres(whenAvailable(strings[8]).map(s -> Arrays.stream(s.split(",")).collect(Collectors.toList())).orElse(Collections.emptyList()));
            }

        };
        return getModelStream(() -> inputStreamsProvider.titleBasicInputStream(), titleBasicsModelConverter);
    }

    public Stream<TitleCrew> titleCrewStream() {
        ModelConverter<TitleCrew> titleBasicsModelConverter = new ModelConverter<TitleCrew>("tconst") {

            @Override
            protected TitleCrew convert(String[] strings) {
                return new TitleCrew()
                        .setTitleId(stringToId(strings[0], "tt"))
                        .setDirectors(stringToIds(strings[1], "nm"))
                        .setWriters(stringToIds(strings[2], "nm"));
            }

        };
        return getModelStream(() -> inputStreamsProvider.titleCrewInputStream(), titleBasicsModelConverter);
    }

    public Stream<TitleEpisode> titleEpisodeStream() {
        ModelConverter<TitleEpisode> titleEpisodeModelConverter = new ModelConverter<TitleEpisode>("tconst") {
            @Override
            protected TitleEpisode convert(String[] strings) {
                return new TitleEpisode()
                        .setTitleId(stringToId(strings[0], "tt"))
                        .setParentTitleId(stringToId(strings[1], "tt"))
                        .setSeasonNumber(stringToInteger(strings[2]))
                        .setEpisodeNumber(stringToInteger(strings[3]));
            }
        };

        return getModelStream(() -> inputStreamsProvider.titleEpisodeInputStream(), titleEpisodeModelConverter);
    }

    public Stream<TitlePrincipals> titlePrincipalsStream() {
        ModelConverter<TitlePrincipals> titlePrincipalsModelConverter = new ModelConverter<TitlePrincipals>("tconst") {

            @Override
            protected TitlePrincipals convert(String[] strings) {
                return new TitlePrincipals()
                        .setTitleId(stringToId(strings[0], "tt"))
                        .setOrdering(stringToInteger(strings[1]))
                        .setNameId(stringToId(strings[2], "nm"))
                        .setCategory(whenAvailable(strings[3]).orElse(null))
                        .setJob(whenAvailable(strings[4]).orElse(null))
                        .setCharacters(parseCharacters(strings[5]));
            }

            private List<String> parseCharacters(String source) {

                if (!whenAvailable(source).isPresent()) {
                    return new ArrayList<>();
                }

                List<String> characters = new ArrayList<>();

                String src = source.trim().substring(1, source.length() - 1).replace("\\s", " ");

                boolean stringOpened = false;
                int prevChar = -1;

                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < src.length(); i++) {
                    char c = src.charAt(i);

                    if (c == '"' && prevChar != '\\' && !stringOpened) {
                        stringOpened = true;
                    } else if (c == '"' && prevChar != '\\') {
                        stringOpened = false;
                        characters.add(sb.toString().replace("  ", " "));
                        sb = new StringBuilder();
                    }

                    if (c == '"' && prevChar == '\\' && stringOpened) {
                        sb.append(c);
                    } else if (prevChar == '\\' && (c == ' ' || Character.isAlphabetic(c)) && stringOpened) {
                        sb.append(" / ").append(c);
                    } else if (prevChar == '\\' && stringOpened) {
                        sb.append(" ").append(c);
                    } else if (stringOpened && c != '\\' && c != '"') {
                        sb.append(c);
                    }


                    prevChar = c;
                }

                return characters;
            }
        };

        return getModelStream(() -> inputStreamsProvider.titlePrincipalsInputStream(), titlePrincipalsModelConverter);
    }

    public Stream<TitleRatings> titleRatingsStream() {
        ModelConverter<TitleRatings> titleRatingsModelConverter = new ModelConverter<TitleRatings>("tconst") {
            @Override
            protected TitleRatings convert(String[] strings) {
                return new TitleRatings()
                        .setTitleId(stringToId(strings[0], "tt"))
                        .setAverageRating(whenAvailable(strings[1]).map(BigDecimal::new).orElse(null))
                        .setNumVotes(stringToInteger(strings[2]));
            }
        };
        return getModelStream(() -> inputStreamsProvider.titleRatingsInputStream(), titleRatingsModelConverter);
    }

    private <T extends Serializable> Stream<T> getModelStream(Supplier<InputStream> inputStreamSupplier, ModelConverter<T> converter) {
        return StreamSupport
                .stream(new ModelSplitIterator<>(inputStreamSupplier, converter), false)
                .filter(Objects::nonNull);
    }

    public static class InputStreamsProvider {
        public InputStream namesInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/name.basics.tsv.gz");
        }

        public InputStream titleAkasInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/title.akas.tsv.gz");
        }

        public InputStream titleBasicInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/title.basics.tsv.gz");
        }

        public InputStream titleCrewInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/title.crew.tsv.gz");
        }

        public InputStream titleEpisodeInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/title.episode.tsv.gz");
        }

        public InputStream titlePrincipalsInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/title.principals.tsv.gz");
        }

        public InputStream titleRatingsInputStream() {
            return gzipStreamFromUrl("https://datasets.imdbws.com/title.ratings.tsv.gz");
        }

        private InputStream gzipStreamFromUrl(String url) {
            try {
                return new GZIPInputStream(new URL(url).openStream());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ModelSplitIterator<T extends Serializable> implements Spliterator<T> {
        private TsvParser parser;

        private ModelConverter<T> converter;

        private Supplier<InputStream> inputStreamSupplier;

        private InputStream is;

        protected ModelSplitIterator(Supplier<InputStream> inputStreamSupplier, ModelConverter<T> converter) {
            this.converter = converter;
            this.inputStreamSupplier = inputStreamSupplier;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            try {
                if (is == null) {
                    TsvParserSettings settings = new TsvParserSettings();
                    settings.setMaxCharsPerColumn(4096 * 100);
                    this.parser = new TsvParser(settings);
                    is = inputStreamSupplier.get();
                    parser.beginParsing(is);
                }

                String[] strings = parser.parseNext();

                if (strings == null) {
                    return false;
                }

                if (converter.isFirstLine(strings)) {
                    action.accept(null);
                    return true;
                }

                action.accept(converter.convert(strings));

                return true;
            } catch (Exception e) {
                parser.stopParsing();
                throw new RuntimeException(e);
            }
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public int characteristics() {
            return DISTINCT | IMMUTABLE;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }
    }

    private abstract static class ModelConverter<T extends Serializable> {
        private String firstColumn;

        protected ModelConverter(String firstColumn) {
            this.firstColumn = firstColumn;
        }

        protected boolean isFirstLine(String[] strings) {
            return firstColumn.equals(strings[0]);
        }

        protected abstract T convert(String[] strings);

        protected Integer stringToInteger(String str) {
            return whenAvailable(str).map(Integer::valueOf).orElse(null);
        }

        protected String whenAvailableString(String string) {
            return whenAvailable(string).orElse(null);
        }

        protected Optional<String> whenAvailable(String string) {
            if (string == null) {
                return Optional.empty();
            }

            String trim = string.trim();
            if ("".equals(trim) || "\\N".equals(trim)) {
                return Optional.empty();
            }

            return Optional.of(trim);
        }

        protected Long stringToId(String string, String prefix) {
            return whenAvailable(string)
                    .map(s -> s.replace(prefix, ""))
                    .map(Long::valueOf)
                    .orElse(null);
        }

        protected Set<Long> stringToIds(String string, String prefix) {
            return whenAvailable(string)
                    .map(
                            s ->
                                    Arrays
                                            .stream(s.split(","))
                                            .map(spl -> stringToId(spl, prefix))
                                            .collect(Collectors.toSet())
                    )
                    .orElse(new HashSet<>());
        }
    }
}
