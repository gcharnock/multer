use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::future;
use futures_util::stream::{Stream, TryStreamExt};
use http::HeaderMap;
use spin::mutex::spin::SpinMutex as Mutex;
#[cfg(feature = "tokio-io")]
use {tokio::io::AsyncRead, tokio_util::io::ReaderStream};

use crate::buffer::StreamBuffer;
use crate::constraints::Constraints;
use crate::content_disposition::ContentDisposition;
use crate::error::Error;
use crate::field::Field;
use crate::{constants, helpers, Result};

/// Represents the implementation of `multipart/form-data` formatted data.
///
/// This will parse the source stream into [`Field`] instances via
/// [`next_field()`](Self::next_field).
///
/// # Field Exclusivity
///
/// A `Field` represents a raw, self-decoding stream into multipart data. As
/// such, only _one_ `Field` from a given `Multipart` instance may be live at
/// once. That is, a `Field` emitted by `next_field()` must be dropped before
/// calling `next_field()` again. Failure to do so will result in an error.
///
/// ```rust
/// use std::convert::Infallible;
///
/// use bytes::Bytes;
/// use futures_util::stream::once;
/// use multer::Multipart;
///
/// # async fn run() {
/// let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
///     name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--\r\n";
///
/// let stream = once(async move { Result::<Bytes, Infallible>::Ok(Bytes::from(data)) });
/// let mut multipart = Multipart::new(stream, "X-BOUNDARY");
///
/// let field1 = multipart.next_field().await;
/// let field2 = multipart.next_field().await;
///
/// assert!(field2.is_err());
/// # }
/// # tokio::runtime::Runtime::new().unwrap().block_on(run());
/// ```
///
/// # Examples
///
/// ```
/// use std::convert::Infallible;
///
/// use bytes::Bytes;
/// use futures_util::stream::once;
/// use multer::Multipart;
///
/// # async fn run() {
/// let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
///     name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--\r\n";
///
/// let stream = once(async move { Result::<Bytes, Infallible>::Ok(Bytes::from(data)) });
/// let mut multipart = Multipart::new(stream, "X-BOUNDARY");
///
/// while let Some(field) = multipart.next_field().await.unwrap() {
///     println!("Field: {:?}", field.text().await)
/// }
/// # }
/// # tokio::runtime::Runtime::new().unwrap().block_on(run());
/// ```
#[derive(Debug)]
pub struct Multipart<'r> {
    state: Arc<Mutex<MultipartState<'r>>>,
}

#[derive(Debug)]
pub(crate) struct MultipartState<'r> {
    pub(crate) buffer: StreamBuffer<'r>,
    pub(crate) boundary: String,
    pub(crate) stage: StreamingStage,
    pub(crate) next_field_idx: usize,
    pub(crate) curr_field_name: Option<String>,
    pub(crate) curr_field_size_limit: u64,
    pub(crate) curr_field_size_counter: u64,
    pub(crate) constraints: Constraints,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamingStage {
    FindingFirstBoundary,
    ReadingBoundary,
    DeterminingBoundaryType,
    ReadingTransportPadding,
    ReadingFieldHeaders,
    ReadingFieldData,
    Eof,
}

impl<'r> Multipart<'r> {
    /// Construct a new `Multipart` instance with the given [`Bytes`] stream and
    /// the boundary.
    pub fn new<S, O, E, B>(stream: S, boundary: B) -> Self
    where
        S: Stream<Item = Result<O, E>> + Send + 'r,
        O: Into<Bytes> + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'r,
        B: Into<String>,
    {
        Multipart::with_constraints(stream, boundary, Constraints::default())
    }

    /// Construct a new `Multipart` instance with the given [`Bytes`] stream and
    /// the boundary.
    pub fn with_constraints<S, O, E, B>(stream: S, boundary: B, constraints: Constraints) -> Self
    where
        S: Stream<Item = Result<O, E>> + Send + 'r,
        O: Into<Bytes> + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'r,
        B: Into<String>,
    {
        let stream = stream
            .map_ok(|b| b.into())
            .map_err(|err| Error::StreamReadFailed(err.into()));

        Multipart {
            state: Arc::new(Mutex::new(MultipartState {
                buffer: StreamBuffer::new(stream, constraints.size_limit.whole_stream),
                boundary: boundary.into(),
                stage: StreamingStage::FindingFirstBoundary,
                next_field_idx: 0,
                curr_field_name: None,
                curr_field_size_limit: constraints.size_limit.per_field,
                curr_field_size_counter: 0,
                constraints,
            })),
        }
    }

    /// Construct a new `Multipart` instance with the given [`AsyncRead`] reader
    /// and the boundary.
    ///
    /// # Optional
    ///
    /// This requires the optional `tokio-io` feature to be enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use multer::Multipart;
    ///
    /// # async fn run() {
    /// let data =
    ///     "--X-BOUNDARY\r\nContent-Disposition: form-data; name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--\r\n";
    /// let reader = data.as_bytes();
    /// let mut multipart = Multipart::with_reader(reader, "X-BOUNDARY");
    ///
    /// while let Some(mut field) = multipart.next_field().await.unwrap() {
    ///     while let Some(chunk) = field.chunk().await.unwrap() {
    ///         println!("Chunk: {:?}", chunk);
    ///     }
    /// }
    /// # }
    /// # tokio::runtime::Runtime::new().unwrap().block_on(run());
    /// ```
    #[cfg(feature = "tokio-io")]
    #[cfg_attr(nightly, doc(cfg(feature = "tokio-io")))]
    pub fn with_reader<R, B>(reader: R, boundary: B) -> Self
    where
        R: AsyncRead + Unpin + Send + 'r,
        B: Into<String>,
    {
        let stream = ReaderStream::new(reader);
        Multipart::new(stream, boundary)
    }

    /// Construct a new `Multipart` instance with the given [`AsyncRead`] reader
    /// and the boundary.
    ///
    /// # Optional
    ///
    /// This requires the optional `tokio-io` feature to be enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use multer::Multipart;
    ///
    /// # async fn run() {
    /// let data =
    ///     "--X-BOUNDARY\r\nContent-Disposition: form-data; name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--\r\n";
    /// let reader = data.as_bytes();
    /// let mut multipart = Multipart::with_reader(reader, "X-BOUNDARY");
    ///
    /// while let Some(mut field) = multipart.next_field().await.unwrap() {
    ///     while let Some(chunk) = field.chunk().await.unwrap() {
    ///         println!("Chunk: {:?}", chunk);
    ///     }
    /// }
    /// # }
    /// # tokio::runtime::Runtime::new().unwrap().block_on(run());
    /// ```
    #[cfg(feature = "tokio-io")]
    #[cfg_attr(nightly, doc(cfg(feature = "tokio-io")))]
    pub fn with_reader_with_constraints<R, B>(reader: R, boundary: B, constraints: Constraints) -> Self
    where
        R: AsyncRead + Unpin + Send + 'r,
        B: Into<String>,
    {
        let stream = ReaderStream::new(reader);
        Multipart::with_constraints(stream, boundary, constraints)
    }

    /// Yields the next [`Field`] if available.
    ///
    /// Any previous `Field` returned by this method must be dropped before
    /// calling this method or [`Multipart::next_field_with_idx()`] again. See
    /// [field-exclusivity](#field-exclusivity) for details.
    pub async fn next_field(&mut self) -> Result<Option<Field<'r>>> {
        future::poll_fn(|cx| self.poll_next_field(cx)).await
    }

    /// Yields the next [`Field`] if available.
    ///
    /// Any previous `Field` returned by this method must be dropped before
    /// calling this method or [`Multipart::next_field_with_idx()`] again. See
    /// [field-exclusivity](#field-exclusivity) for details.
    ///
    /// This method is available since version 2.1.0.
    pub fn poll_next_field(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Field<'r>>>> {
        // This is consistent as we have an `&mut` and `Field` is not `Clone`.
        // Here, we are guaranteeing that the returned `Field` will be the
        // _only_ field with access to the multipart parsing state. This ensure
        // that lock failure can never occur. This is effectively a dynamic
        // version of passing an `&mut` of `self` to the `Field`.
        if Arc::strong_count(&self.state) != 1 {
            return Poll::Ready(Err(Error::LockFailure));
        }

        debug_assert_eq!(Arc::strong_count(&self.state), 1);
        debug_assert!(self.state.try_lock().is_some(), "expected exlusive lock");
        let mut lock = match self.state.try_lock() {
            Some(lock) => lock,
            None => return Poll::Ready(Err(Error::LockFailure)),
        };

        let state = &mut *lock;
        if state.stage == StreamingStage::Eof {
            return Poll::Ready(Ok(None));
        }

        let mut stream_polled = false;

        loop {
            match Self::parse_uptil_next_field(state)? {
                ParseUpToNextFieldResult::Done => return Poll::Ready(Ok(None)),
                ParseUpToNextFieldResult::NeedMore => {
                    if stream_polled {
                        // If we have polled the stream once and we still need more we must now wait
                        return Poll::Pending;
                    } else {
                        if state.buffer.eof {
                            // We need more but the stream has ended
                            return Poll::Ready(Err(Error::IncompleteStream));
                        }

                        // Correctness: poll_stream polls until either eof or the inner stream
                        // returns Poll::Pending so if it returns Ok(()) we can safely assume it has
                        // registered to be woken up
                        if let Err(err) = state.buffer.poll_stream(cx) {
                            return Poll::Ready(Err(Error::StreamReadFailed(err.into())));
                        }
                        stream_polled = true;
                    }
                }
                ParseUpToNextFieldResult::Field {
                    headers,
                    field_idx,
                    content_disposition,
                } => {
                    drop(lock); // The lock will be dropped anyway, but let's be explicit.
                    let field = Field::new(self.state.clone(), headers, field_idx, content_disposition);
                    return Poll::Ready(Ok(Some(field)));
                }
            }
        }
    }

    /// parse the bytes from the buffer into state, looking for the next field. If the field is not
    /// found, stop and request more bytes to be added to the buffer
    fn parse_uptil_next_field(state: &mut MultipartState<'_>) -> Result<ParseUpToNextFieldResult> {
        if state.stage == StreamingStage::FindingFirstBoundary {
            let boundary = &state.boundary;
            let boundary_deriv = format!("{}{}", constants::BOUNDARY_EXT, boundary);
            match state.buffer.read_to(boundary_deriv.as_bytes()) {
                Some(_) => state.stage = StreamingStage::ReadingBoundary,
                None => return Ok(ParseUpToNextFieldResult::NeedMore),
            }
        }

        // The previous field did not finish reading its data.
        if state.stage == StreamingStage::ReadingFieldData {
            match state
                .buffer
                .read_field_data(state.boundary.as_str(), state.curr_field_name.as_deref())?
            {
                Some((done, bytes)) => {
                    state.curr_field_size_counter += bytes.len() as u64;

                    if state.curr_field_size_counter > state.curr_field_size_limit {
                        return Err(Error::FieldSizeExceeded {
                            limit: state.curr_field_size_limit,
                            field_name: state.curr_field_name.clone(),
                        });
                    }

                    if done {
                        state.stage = StreamingStage::ReadingBoundary;
                    } else {
                        return Ok(ParseUpToNextFieldResult::NeedMore);
                    }
                }
                None => {
                    return Ok(ParseUpToNextFieldResult::NeedMore);
                }
            }
        }

        if state.stage == StreamingStage::ReadingBoundary {
            let boundary = &state.boundary;
            let boundary_deriv_len = constants::BOUNDARY_EXT.len() + boundary.len();

            let boundary_bytes = match state.buffer.read_exact(boundary_deriv_len) {
                Some(bytes) => bytes,
                None => return Ok(ParseUpToNextFieldResult::NeedMore),
            };

            if &boundary_bytes[..] == format!("{}{}", constants::BOUNDARY_EXT, boundary).as_bytes() {
                state.stage = StreamingStage::DeterminingBoundaryType;
            } else {
                return Err(Error::IncompleteStream);
            }
        }

        if state.stage == StreamingStage::DeterminingBoundaryType {
            let ext_len = constants::BOUNDARY_EXT.len();
            let next_bytes = match state.buffer.peek_exact(ext_len) {
                Some(bytes) => bytes,
                None => return Ok(ParseUpToNextFieldResult::NeedMore),
            };

            if next_bytes == constants::BOUNDARY_EXT.as_bytes() {
                state.stage = StreamingStage::Eof;
                return Ok(ParseUpToNextFieldResult::Done);
            } else {
                state.stage = StreamingStage::ReadingTransportPadding;
            }
        }

        if state.stage == StreamingStage::ReadingTransportPadding {
            if !state.buffer.advance_past_transport_padding() {
                return Ok(ParseUpToNextFieldResult::NeedMore);
            }

            let crlf_len = constants::CRLF.len();
            let crlf_bytes = match state.buffer.read_exact(crlf_len) {
                Some(bytes) => bytes,
                None => return Ok(ParseUpToNextFieldResult::NeedMore),
            };

            if &crlf_bytes[..] == constants::CRLF.as_bytes() {
                state.stage = StreamingStage::ReadingFieldHeaders;
            } else {
                return Err(Error::IncompleteStream);
            }
        }

        if state.stage == StreamingStage::ReadingFieldHeaders {
            let header_bytes = match state.buffer.read_until(constants::CRLF_CRLF.as_bytes()) {
                Some(bytes) => bytes,
                None => return Ok(ParseUpToNextFieldResult::NeedMore),
            };

            let mut headers = [httparse::EMPTY_HEADER; constants::MAX_HEADERS];

            let headers = match httparse::parse_headers(&header_bytes, &mut headers).map_err(Error::ReadHeaderFailed)? {
                httparse::Status::Complete((_, raw_headers)) => {
                    match helpers::convert_raw_headers_to_header_map(raw_headers) {
                        Ok(headers) => headers,
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }
                httparse::Status::Partial => {
                    return Err(Error::IncompleteHeaders);
                }
            };

            state.stage = StreamingStage::ReadingFieldData;

            let field_idx = state.next_field_idx;
            state.next_field_idx += 1;

            let content_disposition = ContentDisposition::parse(&headers);
            let field_size_limit = state
                .constraints
                .size_limit
                .extract_size_limit_for(content_disposition.field_name.as_deref());

            state.curr_field_name = content_disposition.field_name.clone();
            state.curr_field_size_limit = field_size_limit;
            state.curr_field_size_counter = 0;

            let field_name = content_disposition.field_name.as_deref();
            if !state.constraints.is_it_allowed(field_name) {
                return Err(Error::UnknownField {
                    field_name: field_name.map(str::to_owned),
                });
            }

            return Ok(ParseUpToNextFieldResult::Field {
                headers,
                field_idx,
                content_disposition,
            });
        }
        /*
            Should be unreacable. It's not clear what would be safe to do
            if this is reached.
            * if we loop back to the start, it's not any more "obvious" that progress
              can be made that that this is unreachable. Similar issue for calling
              the waker ourselves and returning Poll::Pending
            * if we have the caller return Poll::Pending, we may not have not polled the
              underlying stream which means we might never get woken up again
            * if we return NeedMore we may end up buffering more than we need, using memory.
        */
        unreachable!()
    }

    /// Yields the next [`Field`] with their positioning index as a tuple
    /// `(`[`usize`]`, `[`Field`]`)`.
    ///
    /// Any previous `Field` returned by this method must be dropped before
    /// calling this method or [`Multipart::next_field()`] again. See
    /// [field-exclusivity](#field-exclusivity) for details.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::convert::Infallible;
    ///
    /// use bytes::Bytes;
    /// use futures_util::stream::once;
    /// use multer::Multipart;
    ///
    /// # async fn run() {
    /// let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
    ///     name=\"my_text_field\"\r\n\r\nabcd\r\n--X-BOUNDARY--\r\n";
    ///
    /// let stream = once(async move { Result::<Bytes, Infallible>::Ok(Bytes::from(data)) });
    /// let mut multipart = Multipart::new(stream, "X-BOUNDARY");
    ///
    /// while let Some((idx, field)) = multipart.next_field_with_idx().await.unwrap() {
    ///     println!("Index: {:?}, Content: {:?}", idx, field.text().await)
    /// }
    /// # }
    /// # tokio::runtime::Runtime::new().unwrap().block_on(run());
    /// ```
    pub async fn next_field_with_idx(&mut self) -> Result<Option<(usize, Field<'r>)>> {
        self.next_field().await.map(|f| f.map(|field| (field.index(), field)))
    }
}

enum ParseUpToNextFieldResult {
    NeedMore,
    Done,
    Field {
        headers: HeaderMap,
        field_idx: usize,
        content_disposition: ContentDisposition,
    },
}
